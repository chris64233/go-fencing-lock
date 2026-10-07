package fencinglock

import (
	"sort"
	"time"
)

// SetClock 注入自定义时钟，主要用于过期相关测试；传入 nil 恢复墙钟。
func (s *Service) SetClock(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if now == nil {
		s.now = time.Now
		return
	}
	s.now = now
}

// GetResource 查询单个资源的当前 token、持有者、组合关系与失效原因。
func (s *Service) GetResource(id string) ResourceInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.scanExpiredLocked(now)

	r, ok := s.resources[id]
	if !ok {
		return ResourceInfo{ResourceID: id, Reason: ReasonNeverAcquired}
	}
	info := ResourceInfo{
		ResourceID: id,
		Token:      r.token,
		Holder:     r.holder,
		ExpiresAt:  r.expiresAt,
	}
	switch {
	case r.holder == "":
		info.Reason = r.lastReason
	case r.groupID != 0:
		g := s.groups[r.groupID]
		if g != nil && g.active {
			info.Active = true
			info.GroupID = r.groupID
		} else {
			info.Reason = ReasonTakenOver
		}
	default:
		info.Active = true
	}
	return info
}

// ListResources 按资源编号稳定顺序返回全部已知资源状态。
func (s *Service) ListResources() []ResourceInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.scanExpiredLocked(now)

	ids := make([]string, 0, len(s.resources))
	for id := range s.resources {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := make([]ResourceInfo, 0, len(ids))
	for _, id := range ids {
		r := s.resources[id]
		info := ResourceInfo{
			ResourceID: id,
			Token:      r.token,
			Holder:     r.holder,
			ExpiresAt:  r.expiresAt,
		}
		switch {
		case r.holder == "":
			info.Reason = r.lastReason
		case r.groupID != 0:
			g := s.groups[r.groupID]
			if g != nil && g.active {
				info.Active = true
				info.GroupID = r.groupID
			} else {
				info.Reason = ReasonTakenOver
			}
		default:
			info.Active = true
		}
		out = append(out, info)
	}
	return out
}

// GetGroup 查询一个组合租约版本的状态与整组 token。
func (s *Service) GetGroup(gid int64) (GroupInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.scanExpiredLocked(now)

	g, ok := s.groups[gid]
	if !ok {
		return GroupInfo{}, ErrNotFound
	}
	info := GroupInfo{
		GroupID:   g.id,
		Holder:    g.holder,
		ExpiresAt: g.expiresAt,
		Resources: append([]string(nil), g.resources...),
		Tokens:    map[string]int64{},
	}
	for _, id := range g.resources {
		if r := s.resources[id]; r != nil {
			info.Tokens[id] = r.token
		}
	}
	if g.active {
		if now.Before(g.expiresAt) {
			info.Active = true
		} else {
			info.Reason = ReasonExpired
		}
	} else {
		info.Reason = g.invalidReason
	}
	return info, nil
}

// ScanExpired 主动运行一次过期扫描，回收所有到期租约
// （组合租约按整组失效处理），返回被回收的资源编号。
func (s *Service) ScanExpired() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()

	var expired []string
	for id, r := range s.resources {
		if r.holder != "" && r.expired(now) {
			expired = append(expired, id)
		}
	}
	s.scanExpiredLocked(now)
	sort.Strings(expired)
	return expired
}
