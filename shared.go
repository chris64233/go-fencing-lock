package fencinglock

import (
	"sort"
	"time"
)

// ReadLease describes a granted shared read lease. Read leases carry their
// own holder, deadline and per-resource read version; they never receive a
// fencing token and can never perform protected writes.
type ReadLease struct {
	Resource  string
	Holder    string
	Version   int64
	ExpiresAt time.Time
}

// UpgradeReceipt is returned by RequestUpgrade and must be presented to
// ConfirmUpgrade. It pins the frozen resource version so stale receipts can
// never mint or overwrite an exclusive fencing token.
type UpgradeReceipt struct {
	Resource      string
	UpgradeID     string
	Applicant     string
	FrozenVersion int64
	FrozenReaders []string
}

// AcquireReadRequest requests a shared read lease.
type AcquireReadRequest struct {
	Resource  string
	Holder    string
	TTL       time.Duration
	RequestID string
}

// RenewReadRequest renews a shared read lease.
type RenewReadRequest struct {
	Resource  string
	Holder    string
	TTL       time.Duration
	RequestID string
}

// ReleaseReadRequest releases a shared read lease.
type ReleaseReadRequest struct {
	Resource  string
	Holder    string
	RequestID string
}

// RequestUpgradeRequest asks to upgrade the caller's read lease into an
// exclusive write lease. The request freezes the resource version and the
// current read holder set; while the upgrade is pending no new read leases
// are granted.
type RequestUpgradeRequest struct {
	Resource  string
	Holder    string
	UpgradeID string
	RequestID string
}

// ConfirmUpgradeRequest atomically completes a pending upgrade once every
// other frozen read lease has been released or has expired. The applicant's
// own read lease must still be active: releasing it first and then trying to
// grab the write lease is rejected.
type ConfirmUpgradeRequest struct {
	Resource      string
	Holder        string
	UpgradeID     string
	FrozenVersion int64
	TTL           time.Duration
	RequestID     string
}

// CancelUpgradeRequest cancels a pending upgrade. Other read holders are
// left untouched and the applicant's own read lease keeps its original
// deadline.
type CancelUpgradeRequest struct {
	Resource  string
	Holder    string
	UpgradeID string
	RequestID string
}

// AcquireRead grants a shared read lease. It fails while an exclusive write
// lease is active, while an intact composite lease owns the resource, or
// while an upgrade is pending. Re-acquiring with the same holder refreshes
// the deadline and read version.
func (s *Service) AcquireRead(req AcquireReadRequest) (*ReadLease, error) {
	if req.Resource == "" || req.Holder == "" || req.TTL <= 0 {
		return nil, ErrInvalidRequest
	}
	content := requestContent{
		resources: []string{req.Resource},
		holder:    req.Holder,
		ttl:       req.TTL,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "read-acquire", content: content}
	rec, replayed, err := s.replayOrBegin(rec, "read-acquire", req.RequestID, content)
	if err != nil {
		return nil, err
	}
	if replayed {
		return rec.readOut, rec.err
	}

	now := s.now()
	l := s.getOrCreateResource(req.Resource)

	if l.compositeID != "" && !l.expired(now) {
		c := s.composites[l.compositeID]
		if c != nil && !c.released && groupIntact(s, c, now) == nil {
			rec.err = ErrUnavailable
			return nil, rec.err
		}
	}
	if l.active(now) {
		// An active exclusive write lease blocks every read lease.
		rec.err = ErrUnavailable
		return nil, rec.err
	}
	l.pruneReaders(now)
	if l.upgrade != nil {
		rec.err = ErrUpgradePending
		return nil, rec.err
	}

	out := s.grantRead(req.Resource, l, req.Holder, req.TTL, now)
	rec.readOut = out
	return out, nil
}

// grantRead records a read lease with the next per-resource read version.
func (s *Service) grantRead(name string, l *lease, holder string, ttl time.Duration, now time.Time) *ReadLease {
	if l.readers == nil {
		l.readers = make(map[string]*readLease)
	}
	l.readVersion++
	l.version++
	r := &readLease{version: l.readVersion, expiresAt: now.Add(ttl)}
	l.readers[holder] = r
	return &ReadLease{
		Resource:  name,
		Holder:    holder,
		Version:   r.version,
		ExpiresAt: r.expiresAt,
	}
}

// RenewRead extends a shared read lease. While an upgrade is pending only
// the applicant may renew; other readers must let their leases expire so the
// upgrade can complete.
func (s *Service) RenewRead(req RenewReadRequest) (*ReadLease, error) {
	if req.Resource == "" || req.Holder == "" || req.TTL <= 0 {
		return nil, ErrInvalidRequest
	}
	content := requestContent{
		resources: []string{req.Resource},
		holder:    req.Holder,
		ttl:       req.TTL,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "read-renew", content: content}
	rec, replayed, err := s.replayOrBegin(rec, "read-renew", req.RequestID, content)
	if err != nil {
		return nil, err
	}
	if replayed {
		return rec.readOut, rec.err
	}

	now := s.now()
	l, ok := s.resources[req.Resource]
	if !ok {
		rec.err = ErrLeaseReleased
		return nil, rec.err
	}
	l.pruneReaders(now)
	r, ok := l.readers[req.Holder]
	if !ok {
		rec.err = ErrLeaseReleased
		return nil, rec.err
	}
	if l.upgrade != nil && l.upgrade.applicant != req.Holder {
		rec.err = ErrUpgradePending
		return nil, rec.err
	}

	l.readVersion++
	l.version++
	r.version = l.readVersion
	r.expiresAt = now.Add(req.TTL)
	if l.upgrade != nil {
		// Keep the applicant's frozen read version in step so its own
		// renew does not invalidate the pending upgrade.
		l.upgrade.readers[req.Holder] = r.version
	}
	out := &ReadLease{
		Resource:  req.Resource,
		Holder:    req.Holder,
		Version:   r.version,
		ExpiresAt: r.expiresAt,
	}
	rec.readOut = out
	return out, nil
}

// ReleaseRead ends a shared read lease. Releasing the applicant's own read
// lease does not cancel a pending upgrade, but it makes any later
// ConfirmUpgrade fail: the applicant cannot release first and then grab the
// exclusive lease.
func (s *Service) ReleaseRead(req ReleaseReadRequest) error {
	if req.Resource == "" || req.Holder == "" {
		return ErrInvalidRequest
	}
	content := requestContent{resources: []string{req.Resource}, holder: req.Holder}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "read-release", content: content}
	rec, replayed, err := s.replayOrBegin(rec, "read-release", req.RequestID, content)
	if err != nil {
		return err
	}
	if replayed {
		return rec.err
	}

	now := s.now()
	l, ok := s.resources[req.Resource]
	if !ok {
		rec.err = ErrLeaseReleased
		return rec.err
	}
	l.pruneReaders(now)
	if _, ok := l.readers[req.Holder]; !ok {
		rec.err = ErrLeaseReleased
		return rec.err
	}
	delete(l.readers, req.Holder)
	l.version++
	return nil
}

// RequestUpgrade registers the caller's intent to upgrade its read lease to
// an exclusive write lease. The resource version and the current read holder
// set are frozen into the receipt. Only one upgrade may be pending per
// resource; the same upgrade id with the same content replays the original
// receipt, and different content conflicts.
func (s *Service) RequestUpgrade(req RequestUpgradeRequest) (*UpgradeReceipt, error) {
	if req.Resource == "" || req.Holder == "" || req.UpgradeID == "" {
		return nil, ErrInvalidRequest
	}
	content := requestContent{
		resources: []string{req.Resource},
		holder:    req.Holder,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "upgrade-request", content: content}
	key := "upg-req:" + req.UpgradeID
	rec, replayed, err := s.replayOrBegin(rec, "upgrade-request", key, content)
	if err != nil {
		return nil, err
	}
	if replayed {
		return rec.upgradeOut, rec.err
	}
	defer func() { s.forgetOnError(key, rec.err) }()

	now := s.now()
	l, ok := s.resources[req.Resource]
	if !ok {
		rec.err = ErrLeaseReleased
		return nil, rec.err
	}
	l.pruneReaders(now)
	if _, ok := l.readers[req.Holder]; !ok {
		rec.err = ErrHolderMismatch
		return nil, rec.err
	}
	if l.upgrade != nil {
		rec.err = ErrUpgradePending
		return nil, rec.err
	}

	frozen := make(map[string]int64, len(l.readers))
	readers := make([]string, 0, len(l.readers))
	for h, r := range l.readers {
		frozen[h] = r.version
		readers = append(readers, h)
	}
	sort.Strings(readers)
	l.upgrade = &pendingUpgrade{
		id:            req.UpgradeID,
		applicant:     req.Holder,
		frozenVersion: l.version,
		readers:       frozen,
	}
	l.version++

	out := &UpgradeReceipt{
		Resource:      req.Resource,
		UpgradeID:     req.UpgradeID,
		Applicant:     req.Holder,
		FrozenVersion: l.upgrade.frozenVersion,
		FrozenReaders: readers,
	}
	rec.upgradeOut = out
	return out, nil
}

// ConfirmUpgrade atomically turns the pending upgrade into an exclusive
// write lease. It succeeds only when every other frozen read lease has been
// released or has expired and the applicant still holds its own active read
// lease. The granted write lease receives the next fencing token; no old
// read version or stale upgrade receipt can mint or overwrite it.
func (s *Service) ConfirmUpgrade(req ConfirmUpgradeRequest) (*Lease, error) {
	if req.Resource == "" || req.Holder == "" || req.UpgradeID == "" || req.TTL <= 0 {
		return nil, ErrInvalidRequest
	}
	content := requestContent{
		resources: []string{req.Resource},
		holder:    req.Holder,
		ttl:       req.TTL,
		version:   req.FrozenVersion,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "upgrade-confirm", content: content}
	key := "upg-cfm:" + req.UpgradeID
	rec, replayed, err := s.replayOrBegin(rec, "upgrade-confirm", key, content)
	if err != nil {
		return nil, err
	}
	if replayed {
		return rec.leaseOut, rec.err
	}
	defer func() { s.forgetOnError(key, rec.err) }()

	now := s.now()
	l, ok := s.resources[req.Resource]
	if !ok {
		rec.err = ErrLeaseReleased
		return nil, rec.err
	}
	u := l.upgrade
	if u == nil {
		rec.err = ErrUpgradeNotFound
		return nil, rec.err
	}
	if u.id != req.UpgradeID || u.applicant != req.Holder || u.frozenVersion != req.FrozenVersion {
		rec.err = ErrUpgradeConflict
		return nil, rec.err
	}

	l.pruneReaders(now)
	r, ok := l.readers[req.Holder]
	if !ok {
		// The applicant released (or lost) its own read lease first; it may
		// not jump the queue and grab the exclusive lease.
		rec.err = ErrLeaseReleased
		return nil, rec.err
	}
	if r.version != u.readers[req.Holder] {
		rec.err = ErrUpgradeConflict
		return nil, rec.err
	}
	for h, v := range u.readers {
		if h == req.Holder {
			continue
		}
		if cur, ok := l.readers[h]; ok && cur.version == v {
			// Another frozen reader is still holding on.
			rec.err = ErrUnavailable
			return nil, rec.err
		}
	}

	// Atomic completion: every read lease (including the applicant's) is
	// consumed and a single exclusive write lease is published.
	l.readers = nil
	l.upgrade = nil
	out := s.grantSingle(req.Resource, l, req.Holder, req.TTL, now)
	l.version++
	rec.leaseOut = out
	return out, nil
}

// CancelUpgrade withdraws a pending upgrade. Only the applicant may cancel.
// No other read holder is removed and the applicant's own read lease keeps
// its original deadline.
func (s *Service) CancelUpgrade(req CancelUpgradeRequest) error {
	if req.Resource == "" || req.Holder == "" || req.UpgradeID == "" {
		return ErrInvalidRequest
	}
	content := requestContent{resources: []string{req.Resource}, holder: req.Holder}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "upgrade-cancel", content: content}
	key := "upg-cancel:" + req.UpgradeID
	rec, replayed, err := s.replayOrBegin(rec, "upgrade-cancel", key, content)
	if err != nil {
		return err
	}
	if replayed {
		return rec.err
	}
	defer func() { s.forgetOnError(key, rec.err) }()

	l, ok := s.resources[req.Resource]
	if !ok {
		rec.err = ErrLeaseReleased
		return rec.err
	}
	u := l.upgrade
	if u == nil || u.id != req.UpgradeID {
		rec.err = ErrUpgradeNotFound
		return rec.err
	}
	if u.applicant != req.Holder {
		rec.err = ErrHolderMismatch
		return rec.err
	}
	l.upgrade = nil
	l.version++
	return nil
}
