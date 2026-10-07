package fencinglock

import "time"

// Lease describes a granted standalone resource lease.
type Lease struct {
	Resource     string
	Holder       string
	FencingToken int64
	ExpiresAt    time.Time
}

// AcquireRequest requests a standalone lease.
type AcquireRequest struct {
	Resource  string
	Holder    string
	TTL       time.Duration
	RequestID string
}

// RenewRequest renews a standalone lease.
type RenewRequest struct {
	Resource  string
	Holder    string
	TTL       time.Duration
	RequestID string
}

// ReleaseRequest releases a standalone lease.
type ReleaseRequest struct {
	Resource  string
	Holder    string
	RequestID string
}

// HandoffRequest transfers a lease to a new holder and issues a new fencing
// token, fencing off writes performed with the old token.
type HandoffRequest struct {
	Resource  string
	From      string
	To        string
	TTL       time.Duration
	RequestID string
}

// WriteRequest is a protected write against a single resource.
type WriteRequest struct {
	Resource     string
	Holder       string
	FencingToken int64
}

// Acquire grants or takes over a single resource lease. An expired lease may
// be taken over by anyone; an active lease held by another holder is rejected.
func (s *Service) Acquire(req AcquireRequest) (*Lease, error) {
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

	rec := &idemRecord{kind: "acquire", content: content}
	rec, replayed, err := s.replayOrBegin(rec, "acquire", req.RequestID, content)
	if err != nil {
		return nil, err
	}
	if replayed {
		return rec.leaseOut, rec.err
	}

	now := s.now()
	l := s.getOrCreateResource(req.Resource)

	// A resource currently bound to an intact composite lease can only be
	// managed as part of that group. Taking one member alone would leave the
	// other members believing the group is still valid.
	if l.compositeID != "" && !l.expired(now) {
		c := s.composites[l.compositeID]
		if c != nil && !c.released && groupIntact(s, c, now) == nil {
			rec.err = ErrUnavailable
			return nil, rec.err
		}
	}

	if l.active(now) && l.holder != req.Holder {
		rec.err = ErrUnavailable
		return nil, rec.err
	}

	out := s.grantSingle(req.Resource, l, req.Holder, req.TTL, now)
	rec.leaseOut = out
	return out, nil
}

// grantSingle issues the next fencing token and records a standalone lease.
func (s *Service) grantSingle(name string, l *lease, holder string, ttl time.Duration, now time.Time) *Lease {
	l.fencingToken++
	l.holder = holder
	l.expiresAt = now.Add(ttl)
	l.compositeID = ""
	l.released = false
	return &Lease{
		Resource:     name,
		Holder:       holder,
		FencingToken: l.fencingToken,
		ExpiresAt:    l.expiresAt,
	}
}

// Renew extends a standalone lease.
func (s *Service) Renew(req RenewRequest) (*Lease, error) {
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

	rec := &idemRecord{kind: "renew", content: content}
	rec, replayed, err := s.replayOrBegin(rec, "renew", req.RequestID, content)
	if err != nil {
		return nil, err
	}
	if replayed {
		return rec.leaseOut, rec.err
	}

	now := s.now()
	l, ok := s.resources[req.Resource]
	if !ok {
		rec.err = ErrLeaseReleased
		return nil, rec.err
	}
	if err := checkStandalone(l, req.Holder, now); err != nil {
		rec.err = err
		return nil, err
	}

	l.expiresAt = now.Add(req.TTL)
	out := &Lease{
		Resource:     req.Resource,
		Holder:       l.holder,
		FencingToken: l.fencingToken,
		ExpiresAt:    l.expiresAt,
	}
	rec.leaseOut = out
	return out, nil
}

// Release ends a standalone lease.
func (s *Service) Release(req ReleaseRequest) error {
	if req.Resource == "" || req.Holder == "" {
		return ErrInvalidRequest
	}
	content := requestContent{resources: []string{req.Resource}, holder: req.Holder}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "release", content: content}
	rec, replayed, err := s.replayOrBegin(rec, "release", req.RequestID, content)
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
	if err := checkStandalone(l, req.Holder, now); err != nil {
		rec.err = err
		return err
	}

	l.lastHolder, l.lastToken = l.holder, l.fencingToken
	l.holder = ""
	l.compositeID = ""
	l.released = true
	return nil
}

// Handoff transfers a lease between holders. An active lease requires the
// current holder's identity; an expired lease may be taken over by anyone.
func (s *Service) Handoff(req HandoffRequest) (*Lease, error) {
	if req.Resource == "" || req.To == "" || req.TTL <= 0 {
		return nil, ErrInvalidRequest
	}
	if req.From == "" {
		return nil, ErrInvalidRequest
	}
	content := requestContent{
		resources: []string{req.Resource},
		holder:    req.To,
		ttl:       req.TTL,
		from:      req.From,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "handoff", content: content}
	rec, replayed, err := s.replayOrBegin(rec, "handoff", req.RequestID, content)
	if err != nil {
		return nil, err
	}
	if replayed {
		return rec.leaseOut, rec.err
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

	switch {
	case l.active(now):
		if l.holder != req.From {
			rec.err = ErrHolderMismatch
			return nil, rec.err
		}
	case l.expired(now):
		if l.holder != req.From {
			rec.err = ErrHolderMismatch
			return nil, rec.err
		}
	default:
		// Never held or already released: nothing to hand off.
		rec.err = ErrLeaseReleased
		return nil, rec.err
	}

	out := s.grantSingle(req.Resource, l, req.To, req.TTL, now)
	rec.leaseOut = out
	return out, nil
}

// Write validates a protected write against a single resource. Composite-owned
// resources must be written through CompositeWrite so the whole group is fenced.
func (s *Service) Write(req WriteRequest) error {
	if req.Resource == "" || req.Holder == "" {
		return ErrInvalidRequest
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	l, ok := s.resources[req.Resource]
	if !ok {
		return ErrLeaseReleased
	}
	if err := checkStandalone(l, req.Holder, now); err != nil {
		return err
	}
	if l.fencingToken != req.FencingToken {
		return ErrFencingToken
	}
	return nil
}

// checkStandalone verifies the resource carries an active, standalone lease
// owned by holder.
func checkStandalone(l *lease, holder string, now time.Time) error {
	switch {
	case l.released:
		return ErrLeaseReleased
	case l.holder == "":
		return ErrLeaseReleased
	case l.expired(now):
		return ErrLeaseExpired
	case l.compositeID != "":
		return ErrCompositeInvalid
	case l.holder != holder:
		return ErrHolderMismatch
	}
	return nil
}
