package fencinglock

import (
	"sort"
	"sync"
	"time"
)

// 资源失效/不可用的原因码，查询结果中使用。
const (
	ReasonUnknown       = ""          // 资源有效，无失效原因
	ReasonNeverAcquired = "never"     // 资源从未被申请
	ReasonExpired       = "expired"   // 租约已过期并被扫描回收
	ReasonReleased      = "released"  // 被持有者主动释放
	ReasonTakenOver     = "takenover" // 持有者已被交接接管
)

// resource 是单个资源的服务端状态。
type resource struct {
	// token 是该资源最近一次颁发的 fencing token；
	// 释放或过期后保留，下一次颁发继续严格递增。
	token int64
	// holder 为当前持有者；空闲时为空。
	holder string
	// expiresAt 是当前租约到期时间；零值表示未持有。
	expiresAt time.Time
	// groupID 为所属有效组合租约的版本号；0 表示单资源租约或空闲。
	groupID int64
	// lastReason 记录资源最近一次变为非持有的原因，便于查询解释。
	lastReason string
}

// expired 判断租约在 now 时刻是否已经过期。
func (r *resource) expired(now time.Time) bool {
	return !r.expiresAt.IsZero() && !now.Before(r.expiresAt)
}

// requestRecord 缓存一次幂等请求的原始内容与结果，便于同号请求重放。
type requestRecord struct {
	kind        opKind
	fingerprint string
	resp        any
	err         error
}

type opKind int

const (
	opAcquire opKind = iota
	opAcquireGroup
	opRenew
	opRenewGroup
	opRelease
	opReleaseGroup
	opTransfer
)

// Service 是内存版租约锁服务。
//
// 所有状态变更都在单一互斥锁内完成（先整体校验、再整体提交），
// 因此单资源申请、组合申请、交接和过期扫描之间不会出现锁顺序死锁，
// 组合申请失败时也不会留下任何半组已取得的可见状态。
type Service struct {
	mu sync.Mutex

	resources map[string]*resource
	// groups 保存组合租约版本的元数据，失效后仍保留用于解释旧 token。
	groups map[int64]*groupMeta

	// seq 是服务级单调递增序号：既是 fencing token 的来源，
	// 也是组合租约版本号的来源，保证二者都不会复用或回退。
	seq int64

	requests map[string]*requestRecord

	now func() time.Time
}

// groupMeta 记录一个组合租约版本持有的资源集合与状态。
type groupMeta struct {
	id        int64
	resources []string
	holder    string
	expiresAt time.Time
	// active 表示该组合版本是否仍完整有效；
	// 任一成员被交接、过期或随组合释放后即变为 false。
	active bool
	// invalidReason 为组合失效原因。
	invalidReason string
}

// NewService 创建空的租约服务。
func NewService() *Service {
	return &Service{
		resources: make(map[string]*resource),
		groups:    make(map[int64]*groupMeta),
		requests:  make(map[string]*requestRecord),
		now:       time.Now,
	}
}

// nextToken 在持锁状态下颁发一个全局递增的 fencing token。
func (s *Service) nextToken() int64 {
	s.seq++
	return s.seq
}

// nextGroupID 在持锁状态下分配组合租约版本号。
func (s *Service) nextGroupID() int64 {
	s.seq++
	return s.seq
}

// normalizeIDs 对资源编号去重并按字典序排序，得到稳定处理顺序。
func normalizeIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// getOrCreate 在持锁状态下取得资源状态，资源首次出现时初始化。
func (s *Service) getOrCreate(id string) *resource {
	r, ok := s.resources[id]
	if !ok {
		r = &resource{lastReason: ReasonNeverAcquired}
		s.resources[id] = r
	}
	return r
}

// clearLeaseLocked 释放单个资源上的租约记录（不影响 fencing token）。
func (s *Service) clearLeaseLocked(r *resource, reason string) {
	r.holder = ""
	r.expiresAt = time.Time{}
	r.groupID = 0
	r.lastReason = reason
}

// breakGroupLocked 使一个组合版本整体失效：
// 其全部成员变为空闲（过期）或被交接方单独持有的形态，
// 成员之间不再互为一组。调用方必须持锁。
func (s *Service) breakGroupLocked(gid int64, reason string) {
	g, ok := s.groups[gid]
	if !ok || !g.active {
		return
	}
	now := s.now()
	g.active = false
	g.invalidReason = reason
	for _, id := range g.resources {
		r := s.resources[id]
		if r == nil || r.groupID != gid {
			continue
		}
		switch reason {
		case ReasonTakenOver:
			// 被交接的资源由调用方另行设置新持有者；其余成员
			// 恢复为旧持有者名下的独立单资源租约，组合版本整体失效。
			// 恰好在此刻过期的成员直接回收。
			if r.holder == g.holder && !r.expired(now) {
				r.groupID = 0
				r.lastReason = ReasonTakenOver
				continue
			}
			s.clearLeaseLocked(r, ReasonTakenOver)
		default:
			s.clearLeaseLocked(r, reason)
		}
	}
}

// scanExpiredLocked 扫描全部资源，回收所有过期租约。
func (s *Service) scanExpiredLocked(now time.Time) {
	// 先按稳定顺序找出需要过期的组，避免在遍历 map 时重入修改。
	groupSeen := map[int64]bool{}
	var ids []string
	for id := range s.resources {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := s.resources[id]
		if !r.expired(now) {
			continue
		}
		if r.groupID != 0 {
			if !groupSeen[r.groupID] {
				groupSeen[r.groupID] = true
				s.breakGroupLocked(r.groupID, ReasonExpired)
			}
			continue
		}
		s.clearLeaseLocked(r, ReasonExpired)
	}
}
