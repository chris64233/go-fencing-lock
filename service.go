package fencinglock

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// DefaultLeaseTTL 是租约的默认有效期。
const DefaultLeaseTTL = 30 * time.Second

var (
	// ErrLockNotFound 表示锁不存在。
	ErrLockNotFound = errors.New("fencinglock: lock not found")
	// ErrLockHeld 表示锁仍被有效租约持有。
	ErrLockHeld = errors.New("fencinglock: lock is held by an active lease")
	// ErrNoTransfer 表示没有待确认的交接。
	ErrNoTransfer = errors.New("fencinglock: no pending transfer")
	// ErrTransferPending 表示已有其他交接请求待确认。
	ErrTransferPending = errors.New("fencinglock: another transfer is already pending")
	// ErrTransferExpired 表示交接未在期限内确认，原租约继续按原期限有效。
	ErrTransferExpired = errors.New("fencinglock: transfer deadline passed")
)

// 失效原因，记录在时间线中。
const (
	ReasonExpired     = "expired"
	ReasonTransferred = "transferred"
)

// StaleVersionError 表示写入或请求携带的围栏版本已过期。
// Got 是调用方携带的旧版本，Current 是锁的当前版本。
type StaleVersionError struct {
	LockName string
	Holder   string
	Got      uint64
	Current  uint64
}

func (e *StaleVersionError) Error() string {
	return fmt.Sprintf("fencinglock: stale version for lock %q from holder %q: got %d, current %d",
		e.LockName, e.Holder, e.Got, e.Current)
}

// NotHolderError 表示版本匹配但持有者不匹配。
type NotHolderError struct {
	LockName string
	Holder   string
	Current  string
}

func (e *NotHolderError) Error() string {
	return fmt.Sprintf("fencinglock: %q is not the holder of lock %q (current holder %q)",
		e.Holder, e.LockName, e.Current)
}

// LeaseExpiredError 表示租约已过期。
type LeaseExpiredError struct {
	LockName  string
	Holder    string
	Version   uint64
	ExpiredAt time.Time
}

func (e *LeaseExpiredError) Error() string {
	return fmt.Sprintf("fencinglock: lease for lock %q (holder %q, version %d) expired at %s",
		e.LockName, e.Holder, e.Version, e.ExpiredAt.Format(time.RFC3339Nano))
}

// TransferConflictError 表示相同 RequestID 的转移请求参数发生变化。
type TransferConflictError struct {
	RequestID string
	Reason    string
}

func (e *TransferConflictError) Error() string {
	return fmt.Sprintf("fencinglock: transfer request %q conflicts: %s", e.RequestID, e.Reason)
}

// HistoryEntry 描述一段持有者任期，失效后记录原因。
type HistoryEntry struct {
	Version       uint64
	Holder        string
	AcquiredAt    time.Time
	InvalidatedAt time.Time
	Reason        string
}

// PendingTransfer 是已发起但未确认的交接。
type PendingTransfer struct {
	RequestID    string
	TargetHolder string
	BaseVersion  uint64
	Deadline     time.Time
}

// TransferRequest 描述一次持有者交接请求。
// RequestID 用于幂等：相同请求返回原结果，锁名或版本变化返回冲突。
type TransferRequest struct {
	RequestID    string
	LockName     string
	Version      uint64
	TargetHolder string
	Deadline     time.Time
	LeaseTTL     time.Duration
}

// Grant 是租约授权或交接确认的结果。
type Grant struct {
	LockName  string
	Holder    string
	Version   uint64
	ExpiresAt time.Time
}

// LockInfo 是锁状态的只读快照，供查询与管理端使用。
type LockInfo struct {
	Name      string
	Holder    string
	Version   uint64
	ExpiresAt time.Time
	Data      string
	Pending   *PendingTransfer
	History   []HistoryEntry
}

type lockState struct {
	holder    string
	version   uint64
	expiresAt time.Time
	data      string
	pending   *PendingTransfer
	history   []HistoryEntry
}

func (l *lockState) expired(now time.Time) bool {
	return !now.Before(l.expiresAt)
}

func (l *lockState) closeTerm(now time.Time, reason string) {
	last := len(l.history) - 1
	if last >= 0 && l.history[last].Reason == "" {
		l.history[last].InvalidatedAt = now
		l.history[last].Reason = reason
	}
}

type transferOutcome struct {
	grant Grant
	err   error
}

// Service 管理带围栏版本的租约锁，支持持有者交接。
type Service struct {
	mu              sync.Mutex
	now             func() time.Time
	defaultLeaseTTL time.Duration
	locks           map[string]*lockState
	requests        map[string]TransferRequest
	outcomes        map[string]transferOutcome
}

// NewService 创建一个租约锁服务。
func NewService() *Service {
	return &Service{
		now:             time.Now,
		defaultLeaseTTL: DefaultLeaseTTL,
		locks:           make(map[string]*lockState),
		requests:        make(map[string]TransferRequest),
		outcomes:        make(map[string]transferOutcome),
	}
}

// Acquire 为 holder 获取锁，返回带递增围栏版本的授权。
// 锁被有效租约持有时返回 ErrLockHeld；租约过期后重新获取会递增版本。
func (s *Service) Acquire(lockName, holder string, ttl time.Duration) (Grant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	if ttl <= 0 {
		ttl = s.defaultLeaseTTL
	}

	st, ok := s.locks[lockName]
	if ok && !st.expired(now) {
		return Grant{}, ErrLockHeld
	}
	if !ok {
		st = &lockState{}
		s.locks[lockName] = st
	} else {
		st.closeTerm(now, ReasonExpired)
	}

	st.version++
	st.holder = holder
	st.expiresAt = now.Add(ttl)
	st.pending = nil
	st.history = append(st.history, HistoryEntry{
		Version:    st.version,
		Holder:     holder,
		AcquiredAt: now,
	})
	return Grant{LockName: lockName, Holder: holder, Version: st.version, ExpiresAt: st.expiresAt}, nil
}

// Write 以持有者的围栏版本写入资源。
// 只有当前版本可以修改资源；旧版本返回 StaleVersionError，
// 明确携带旧版本与当前版本，且不会被记入交接时间线。
func (s *Service) Write(lockName, holder string, version uint64, data string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	st, ok := s.locks[lockName]
	if !ok {
		return ErrLockNotFound
	}
	if version != st.version {
		return &StaleVersionError{LockName: lockName, Holder: holder, Got: version, Current: st.version}
	}
	if holder != st.holder {
		return &NotHolderError{LockName: lockName, Holder: holder, Current: st.holder}
	}
	if st.expired(s.now()) {
		return &LeaseExpiredError{LockName: lockName, Holder: holder, Version: version, ExpiredAt: st.expiresAt}
	}
	st.data = data
	return nil
}

// RequestTransfer 发起持有者交接。请求必须携带当前围栏版本，
// 交接期限内由 ConfirmTransfer 确认后才会生成递增的新版本。
func (s *Service) RequestTransfer(req TransferRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, _, done := s.checkIdempotent(req); done {
		return nil
	}
	if err := s.registerRequest(req); err != nil {
		return err
	}

	st, err := s.transferable(req)
	if err != nil {
		return err
	}
	if st.pending != nil {
		if st.pending.RequestID == req.RequestID {
			return nil
		}
		return ErrTransferPending
	}
	st.pending = &PendingTransfer{
		RequestID:    req.RequestID,
		TargetHolder: req.TargetHolder,
		BaseVersion:  req.Version,
		Deadline:     req.Deadline,
	}
	return nil
}

// ConfirmTransfer 确认交接：旧持有者任期以 transferred 关闭，
// 目标持有者获得递增的围栏版本与新租约。
// 超过交接期限确认会失败，原租约继续按原期限有效。
// 相同请求重复确认返回原结果，锁名或版本变化返回 TransferConflictError。
func (s *Service) ConfirmTransfer(req TransferRequest) (Grant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if grant, err, done := s.checkIdempotent(req); done {
		return grant, err
	}
	if err := s.registerRequest(req); err != nil {
		return Grant{}, err
	}

	grant, err := s.confirmTransfer(req)
	s.outcomes[req.RequestID] = transferOutcome{grant: grant, err: err}
	return grant, err
}

func (s *Service) confirmTransfer(req TransferRequest) (Grant, error) {
	st, err := s.transferable(req)
	if err != nil {
		return Grant{}, err
	}
	p := st.pending
	if p == nil || p.RequestID != req.RequestID {
		return Grant{}, ErrNoTransfer
	}

	now := s.now()
	if now.After(p.Deadline) {
		st.pending = nil
		return Grant{}, ErrTransferExpired
	}

	ttl := req.LeaseTTL
	if ttl <= 0 {
		ttl = s.defaultLeaseTTL
	}
	st.closeTerm(now, ReasonTransferred)
	st.version++
	st.holder = p.TargetHolder
	st.expiresAt = now.Add(ttl)
	st.pending = nil
	st.history = append(st.history, HistoryEntry{
		Version:    st.version,
		Holder:     p.TargetHolder,
		AcquiredAt: now,
	})
	return Grant{LockName: req.LockName, Holder: st.holder, Version: st.version, ExpiresAt: st.expiresAt}, nil
}

func (s *Service) transferable(req TransferRequest) (*lockState, error) {
	st, ok := s.locks[req.LockName]
	if !ok {
		return nil, ErrLockNotFound
	}
	if req.Version != st.version {
		return nil, &StaleVersionError{LockName: req.LockName, Got: req.Version, Current: st.version}
	}
	if st.expired(s.now()) {
		return nil, &LeaseExpiredError{LockName: req.LockName, Holder: st.holder, Version: st.version, ExpiredAt: st.expiresAt}
	}
	return st, nil
}

// checkIdempotent 命中已完成的相同请求时返回原结果。
func (s *Service) checkIdempotent(req TransferRequest) (Grant, error, bool) {
	prev, ok := s.requests[req.RequestID]
	if !ok {
		return Grant{}, nil, false
	}
	if prev.LockName != req.LockName || prev.Version != req.Version || prev.TargetHolder != req.TargetHolder {
		return Grant{}, &TransferConflictError{
			RequestID: req.RequestID,
			Reason:    "same request id with different lock name, version or target holder",
		}, true
	}
	if out, ok := s.outcomes[req.RequestID]; ok {
		return out.grant, out.err, true
	}
	return Grant{}, nil, false
}

func (s *Service) registerRequest(req TransferRequest) error {
	if req.RequestID == "" {
		return errors.New("fencinglock: transfer request id is required")
	}
	if _, ok := s.requests[req.RequestID]; !ok {
		s.requests[req.RequestID] = req
	}
	return nil
}

// Inspect 返回锁的只读快照，包含当前状态与完整交接时间线。
// 资源写入失败不影响状态查询，管理端可依据时间线判断交接先后。
func (s *Service) Inspect(lockName string) (LockInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	st, ok := s.locks[lockName]
	if !ok {
		return LockInfo{}, ErrLockNotFound
	}
	info := LockInfo{
		Name:      lockName,
		Holder:    st.holder,
		Version:   st.version,
		ExpiresAt: st.expiresAt,
		Data:      st.data,
		History:   append([]HistoryEntry(nil), st.history...),
	}
	if st.pending != nil {
		pending := *st.pending
		info.Pending = &pending
	}
	return info, nil
}
