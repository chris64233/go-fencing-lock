package fencinglock

import "time"

// ReadLease describes a granted shared read lease.
type ReadLease struct {
	Resource  string
	Holder    string
	Version   int64
	ExpiresAt time.Time
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

// UpgradeRequest asks to upgrade the caller's read lease into an exclusive
// write lease. RequestID doubles as the upgrade number: replaying the same
// number with the same content returns the original grant, while different
// content under the same number is a conflict.
type UpgradeRequest struct {
	Resource  string
	Holder    string
	RequestID string
}

// UpgradeGrant is the receipt of a pending upgrade. It freezes the read
// version and the reader set observed at request time.
type UpgradeGrant struct {
	Resource      string
	Applicant     string
	UpgradeID     string
	FrozenVersion int64
	FrozenReaders []string
}

// CompleteUpgradeRequest finalizes a pending upgrade once every other frozen
// reader has released or expired. The applicant must still hold its read
// lease; releasing it first forfeits the upgrade.
type CompleteUpgradeRequest struct {
	Resource      string
	Holder        string
	UpgradeID     string
	FrozenVersion int64
	TTL           time.Duration
	RequestID     string
}

// CancelUpgradeRequest withdraws a pending upgrade. Other readers are not
// affected and the applicant's own read lease keeps its original deadline.
type CancelUpgradeRequest struct {
	Resource  string
	Holder    string
	UpgradeID string
	RequestID string
}

// AcquireRead grants a shared read lease. Multiple readers may coexist, but
// no read lease is granted while an exclusive write lease is active or while
// an upgrade is waiting for the reader set to drain.
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
		if c := s.composites[l.compositeID]; c != nil && !c.released && groupIntact(s, c, now) == nil {
			rec.err = ErrUnavailable
			return nil, rec.err
		}
	}
	if l.active(now) {
		rec.err = ErrUnavailable
		return nil, rec.err
	}
	if l.upgrade != nil {
		rec.err = ErrUpgradePending
		return nil, rec.err
	}

	out := s.grantRead(req.Resource, l, req.Holder, req.TTL, now)
	rec.readOut = out
	return out, nil
}

// grantRead records a read lease under the current read version.
func (s *Service) grantRead(name string, l *lease, holder string, ttl time.Duration, now time.Time) *ReadLease {
	if l.readers == nil {
		l.readers = make(map[string]*readLease)
	}
	if _, ok := l.readers[holder]; !ok {
		l.readVersion++
	}
	r := &readLease{holder: holder, expiresAt: now.Add(ttl), version: l.readVersion}
	l.readers[holder] = r
	return &ReadLease{Resource: name, Holder: holder, Version: r.version, ExpiresAt: r.expiresAt}
}

// RenewRead extends an active shared read lease. The lease keeps the read
// version it was issued under.
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
		rec.err = ErrNoReadLease
		return nil, rec.err
	}
	r, err := checkReader(l, req.Holder, now)
	if err != nil {
		rec.err = err
		return nil, err
	}

	r.expiresAt = now.Add(req.TTL)
	out := &ReadLease{Resource: req.Resource, Holder: req.Holder, Version: r.version, ExpiresAt: r.expiresAt}
	rec.readOut = out
	return out, nil
}

// ReleaseRead drops one holder's shared read lease. Other readers are
// unaffected. While an upgrade is pending the release does not advance the
// read version, so the frozen snapshot stays valid for the draining readers.
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
		rec.err = ErrNoReadLease
		return rec.err
	}
	if _, err := checkReader(l, req.Holder, now); err != nil {
		rec.err = err
		return err
	}

	delete(l.readers, req.Holder)
	if l.upgrade == nil {
		l.readVersion++
	}
	return nil
}

// RequestUpgrade registers the caller's intent to upgrade its read lease to
// an exclusive write lease. The request freezes the current read version and
// reader set, and blocks further read acquisitions until the upgrade
// completes or is cancelled.
func (s *Service) RequestUpgrade(req UpgradeRequest) (*UpgradeGrant, error) {
	if req.Resource == "" || req.Holder == "" || req.RequestID == "" {
		return nil, ErrInvalidRequest
	}
	content := requestContent{resources: []string{req.Resource}, holder: req.Holder}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "upgrade-request", content: content}
	rec, replayed, err := s.replayOrBegin(rec, "upgrade-request", req.RequestID, content)
	if err != nil {
		return nil, err
	}
	if replayed {
		return rec.upgradeOut, rec.err
	}

	now := s.now()
	l, ok := s.resources[req.Resource]
	if !ok {
		rec.err = ErrNoReadLease
		return nil, rec.err
	}
	if l.upgrade != nil {
		// A different upgrade number is already pending on this resource.
		rec.err = ErrUpgradeConflict
		return nil, rec.err
	}
	if _, err := checkReader(l, req.Holder, now); err != nil {
		rec.err = err
		return nil, err
	}

	frozen := make(map[string]struct{})
	grant := &UpgradeGrant{
		Resource:      req.Resource,
		Applicant:     req.Holder,
		UpgradeID:     req.RequestID,
		FrozenVersion: l.readVersion,
	}
	for holder := range l.activeReaders(now) {
		frozen[holder] = struct{}{}
		grant.FrozenReaders = append(grant.FrozenReaders, holder)
	}
	sortStrings(grant.FrozenReaders)

	l.upgrade = &upgradeState{
		id:            req.RequestID,
		applicant:     req.Holder,
		frozenVersion: l.readVersion,
		frozenReaders: frozen,
	}
	rec.upgradeOut = grant
	return grant, nil
}

// CompleteUpgrade atomically converts the applicant's read lease into an
// exclusive write lease once every other reader has released or expired. A
// stale upgrade number or frozen version never produces a write lease and
// never touches the current fencing token.
func (s *Service) CompleteUpgrade(req CompleteUpgradeRequest) (*Lease, error) {
	if req.Resource == "" || req.Holder == "" || req.UpgradeID == "" || req.TTL <= 0 {
		return nil, ErrInvalidRequest
	}
	content := requestContent{
		resources: []string{req.Resource},
		holder:    req.Holder,
		ttl:       req.TTL,
		from:      req.UpgradeID,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "upgrade-complete", content: content}
	rec, replayed, err := s.replayOrBegin(rec, "upgrade-complete", req.RequestID, content)
	if err != nil {
		return nil, err
	}
	if replayed {
		return rec.leaseOut, rec.err
	}

	now := s.now()
	l, ok := s.resources[req.Resource]
	if !ok || l.upgrade == nil {
		rec.err = ErrUpgradeNotFound
		return nil, rec.err
	}
	up := l.upgrade
	if up.id != req.UpgradeID || up.applicant != req.Holder {
		rec.err = ErrUpgradeConflict
		return nil, rec.err
	}
	if req.FrozenVersion != up.frozenVersion || l.readVersion != up.frozenVersion {
		rec.err = ErrUpgradeConflict
		return nil, rec.err
	}
	// The applicant must still hold its read lease: releasing it first and
	// then grabbing the write lease is not a valid upgrade.
	if _, err := checkReader(l, req.Holder, now); err != nil {
		rec.err = err
		return nil, err
	}
	// Every other reader must be gone before the exclusive grant.
	for holder := range l.activeReaders(now) {
		if holder != req.Holder {
			rec.err = ErrUnavailable
			return nil, rec.err
		}
	}

	l.readers = nil
	l.upgrade = nil
	l.readVersion++
	out := s.grantSingle(req.Resource, l, req.Holder, req.TTL, now)
	rec.leaseOut = out
	return out, nil
}

// CancelUpgrade withdraws a pending upgrade. Only the applicant may cancel.
// The other readers are left untouched and the applicant's own read lease
// keeps whatever validity its original deadline still allows.
func (s *Service) CancelUpgrade(req CancelUpgradeRequest) error {
	if req.Resource == "" || req.Holder == "" || req.UpgradeID == "" {
		return ErrInvalidRequest
	}
	content := requestContent{
		resources: []string{req.Resource},
		holder:    req.Holder,
		from:      req.UpgradeID,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "upgrade-cancel", content: content}
	rec, replayed, err := s.replayOrBegin(rec, "upgrade-cancel", req.RequestID, content)
	if err != nil {
		return err
	}
	if replayed {
		return rec.err
	}

	l, ok := s.resources[req.Resource]
	if !ok || l.upgrade == nil {
		rec.err = ErrUpgradeNotFound
		return rec.err
	}
	if l.upgrade.id != req.UpgradeID || l.upgrade.applicant != req.Holder {
		rec.err = ErrUpgradeConflict
		return rec.err
	}

	l.upgrade = nil
	l.readVersion++
	return nil
}

// checkReader verifies holder holds an active read lease on l.
func checkReader(l *lease, holder string, now time.Time) (*readLease, error) {
	r, ok := l.readers[holder]
	if !ok {
		return nil, ErrNoReadLease
	}
	if !r.active(now) {
		return nil, ErrLeaseExpired
	}
	return r, nil
}
