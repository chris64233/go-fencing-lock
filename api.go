package fencinglock

import (
	"fmt"
	"time"
)

// ReasonHeld 表示资源正被其他有效持有者占用，申请无法立即满足。
const ReasonHeld = "held"

// UnavailableError 说明申请失败的具体资源与原因；
// errors.Is(err, ErrUnavailable) 恒为 true。
type UnavailableError struct {
	Resource string
	Reason   string
}

func (e *UnavailableError) Error() string {
	return fmt.Sprintf("fencinglock: resource %q unavailable: %s", e.Resource, e.Reason)
}

func (e *UnavailableError) Unwrap() error { return ErrUnavailable }

// AcquireRequest 申请单个资源的租约。
type AcquireRequest struct {
	ResourceID string
	Holder     string
	TTL        time.Duration
	// RequestID 非空时启用幂等：同号必须同内容，否则返回 ErrConflict。
	RequestID string
}

// LeaseResult 是单资源租约申请、续租或交接的结果。
type LeaseResult struct {
	ResourceID string
	Holder     string
	Token      int64
	ExpiresAt  time.Time
}

func cloneLease(r *LeaseResult) *LeaseResult {
	cp := *r
	return &cp
}

// GroupRequest 一次性申请一组资源的组合租约。
type GroupRequest struct {
	ResourceIDs []string
	Holder      string
	TTL         time.Duration
	RequestID   string
}

// GroupRenewRequest 按组合版本续租整组资源。
type GroupRenewRequest struct {
	GroupID   int64
	Holder    string
	TTL       time.Duration
	RequestID string
}

// GroupOpRequest 按组合版本释放整组资源。
type GroupOpRequest struct {
	GroupID   int64
	Holder    string
	RequestID string
}

// GroupResult 是组合租约申请或续租成功后返回的整组信息。
type GroupResult struct {
	GroupID int64
	Holder  string
	// Resources 为去重并按稳定顺序排列后的资源编号。
	Resources []string
	// Tokens 是每个资源各自独立且递增的 fencing token。
	Tokens    map[string]int64
	ExpiresAt time.Time
}

func cloneGroup(g *GroupResult) *GroupResult {
	cp := *g
	cp.Resources = append([]string(nil), g.Resources...)
	cp.Tokens = make(map[string]int64, len(g.Tokens))
	for k, v := range g.Tokens {
		cp.Tokens[k] = v
	}
	return &cp
}

// RenewRequest 续租单个资源。
type RenewRequest struct {
	ResourceID string
	Holder     string
	TTL        time.Duration
	RequestID  string
}

// ReleaseRequest 释放单个资源。
type ReleaseRequest struct {
	ResourceID string
	Holder     string
	RequestID  string
}

// TransferRequest 将资源持有者从 From 交接给 To，并颁发新 token。
type TransferRequest struct {
	ResourceID string
	From       string
	To         string
	TTL        time.Duration
	RequestID  string
}

// WriteRequest 使用单资源租约执行一次受保护写入。
type WriteRequest struct {
	ResourceID string
	Holder     string
	Token      int64
}

// GroupWriteRequest 使用组合租约对整组资源执行受保护写入。
type GroupWriteRequest struct {
	GroupID int64
	Holder  string
	// Tokens 必须恰好覆盖组合持有的全部资源。
	Tokens map[string]int64
}

// ResourceInfo 描述查询时刻单个资源的状态。
type ResourceInfo struct {
	ResourceID string
	Token      int64
	Holder     string
	// GroupID 为所属有效组合租约版本；0 表示单资源租约或空闲。
	GroupID int64
	// Active 表示当前是否存在有效（未过期、未释放、未被接管）的持有关系。
	Active    bool
	ExpiresAt time.Time
	// Reason 在非有效状态时解释失效原因（expired/released/takenover/never）。
	Reason string
}

// GroupInfo 描述一个组合租约版本的状态。
type GroupInfo struct {
	GroupID   int64
	Holder    string
	Resources []string
	Tokens    map[string]int64
	ExpiresAt time.Time
	Active    bool
	Reason    string
}

// replay 查找同号请求；内容指纹不一致时返回 ErrConflict。
func (s *Service) replay(id string, kind opKind, fingerprint string) (any, error, bool) {
	rec, ok := s.requests[id]
	if !ok {
		return nil, nil, false
	}
	if rec.kind != kind || rec.fingerprint != fingerprint {
		return nil, ErrConflict, true
	}
	return rec.resp, rec.err, true
}

func (s *Service) remember(id string, kind opKind, fingerprint string, resp any, err error) {
	if id == "" {
		return
	}
	s.requests[id] = &requestRecord{kind: kind, fingerprint: fingerprint, resp: resp, err: err}
}

func ttlFingerprint(ttl time.Duration) string {
	return fmt.Sprintf("ttl=%d", int64(ttl))
}
