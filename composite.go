package fencinglock

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

// CompositeLease describes a granted composite lease over a resource group.
type CompositeLease struct {
	ID        string
	Resources []string
	Holder    string
	Tokens    map[string]int64
	Version   int64
	ExpiresAt time.Time
}

// CompositeAcquireRequest requests a lease over an entire resource group.
type CompositeAcquireRequest struct {
	Resources []string
	Holder    string
	TTL       time.Duration
	RequestID string
}

// CompositeRenewRequest renews a composite lease as a whole.
type CompositeRenewRequest struct {
	CompositeID string
	Holder      string
	TTL         time.Duration
	RequestID   string
}

// CompositeReleaseRequest releases a composite lease as a whole.
type CompositeReleaseRequest struct {
	CompositeID string
	Holder      string
	RequestID   string
}

// CompositeWriteRequest is a protected write against one member of a composite
// lease. Every member is validated, so a takeover or expiry of any one member
// blocks writes against the others.
type CompositeWriteRequest struct {
	CompositeID  string
	Resource     string
	Holder       string
	FencingToken int64
}

var compositeSeq int64

func nextCompositeID() string {
	n := atomic.AddInt64(&compositeSeq, 1)
	return "cl-" + strconv.FormatInt(n, 10)
}

// CompositeAcquire atomically leases a resource group. Resources are
// de-duplicated and processed in sorted order. The acquisition is verified at
// a single version boundary: if any member cannot be taken, every provisional
// grant is rolled back and no half-group state becomes observable.
func (s *Service) CompositeAcquire(req CompositeAcquireRequest) (*CompositeLease, error) {
	if req.Holder == "" || req.TTL <= 0 {
		return nil, ErrInvalidRequest
	}
	members := normalizeResources(req.Resources)
	if len(members) == 0 {
		return nil, ErrInvalidRequest
	}
	content := requestContent{resources: members, holder: req.Holder, ttl: req.TTL}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "composite-acquire", content: content}
	rec, replayed, err := s.replayOrBegin(rec, "composite-acquire", req.RequestID, content)
	if err != nil {
		return nil, err
	}
	if replayed {
		return rec.compositeOut, rec.err
	}

	now := s.now()

	// Phase 1: verify every member is available before granting anything.
	for _, name := range members {
		l := s.getOrCreateResource(name)
		if err := memberTakeable(l, req.Holder, now); err != nil {
			rec.err = err
			return nil, err
		}
	}

	// Phase 2: provisionally grant the whole group, snapshotting originals so
	// a failure at the final boundary restores still-valid standalone leases.
	snapshot := make(map[string]*lease, len(members))
	for _, name := range members {
		l := s.resources[name]
		clone := *l
		snapshot[name] = &clone
	}

	id := nextCompositeID()
	tokens := make(map[string]int64, len(members))
	expires := now.Add(req.TTL)
	for _, name := range members {
		l := s.resources[name]
		l.fencingToken++
		l.holder = req.Holder
		l.expiresAt = expires
		l.compositeID = id
		l.released = false
		tokens[name] = l.fencingToken
	}

	c := &composite{
		id:      id,
		holder:  req.Holder,
		ttl:     req.TTL,
		expires: expires,
		version: 1,
		members: members,
		tokens:  tokens,
	}

	// Version boundary: nothing in the group may have diverged within this
	// critical section. Re-verify invariants before publishing the group.
	if err := groupIntact(s, c, now); err != nil {
		s.restore(snapshot)
		rec.err = err
		return nil, err
	}
	s.composites[id] = c

	out := compositeView(c)
	rec.compositeOut = out
	return out, nil
}

// memberTakeable decides whether holder can take a member as part of a group:
// unknown resources, released/expired leases and the same holder's standalone
// lease are takeable; another holder's active standalone or composite lease is
// not.
func memberTakeable(l *lease, holder string, now time.Time) error {
	if l.upgrade != nil {
		return ErrUpgradePending
	}
	if len(l.activeReaders(now)) > 0 {
		return ErrUnavailable
	}
	if !l.active(now) {
		return nil
	}
	if l.holder != holder {
		return ErrUnavailable
	}
	return nil
}

// groupIntact validates every member against the recorded composite at one
// version boundary: same group, same holder, same token, still unexpired.
func groupIntact(s *Service, c *composite, now time.Time) error {
	for _, name := range c.members {
		l, ok := s.resources[name]
		if !ok {
			return fmt.Errorf("%w: resource %q vanished", ErrCompositeInvalid, name)
		}
		if l.compositeID != c.id {
			return fmt.Errorf("%w: resource %q left the group", ErrCompositeInvalid, name)
		}
		if l.holder != c.holder {
			return fmt.Errorf("%w: resource %q taken over by %q", ErrCompositeInvalid, name, l.holder)
		}
		if l.expired(now) || l.holder == "" {
			return fmt.Errorf("%w: resource %q expired", ErrCompositeInvalid, name)
		}
		if l.fencingToken != c.tokens[name] {
			return fmt.Errorf("%w: resource %q fencing token advanced", ErrCompositeInvalid, name)
		}
	}
	if c.expires.Before(now) {
		return ErrLeaseExpired
	}
	return nil
}

// CompositeRenew extends every member's lease and bumps the composite version.
func (s *Service) CompositeRenew(req CompositeRenewRequest) (*CompositeLease, error) {
	if req.CompositeID == "" || req.Holder == "" || req.TTL <= 0 {
		return nil, ErrInvalidRequest
	}
	content := requestContent{
		resources: []string{req.CompositeID},
		holder:    req.Holder,
		ttl:       req.TTL,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "composite-renew", content: content}
	rec, replayed, err := s.replayOrBegin(rec, "composite-renew", req.RequestID, content)
	if err != nil {
		return nil, err
	}
	if replayed {
		return rec.compositeOut, rec.err
	}

	now := s.now()
	c, ok := s.composites[req.CompositeID]
	if !ok {
		rec.err = ErrCompositeNotFound
		return nil, rec.err
	}
	if c.released {
		rec.err = ErrLeaseReleased
		return nil, rec.err
	}
	if c.holder != req.Holder {
		rec.err = ErrHolderMismatch
		return nil, rec.err
	}
	if err := groupIntact(s, c, now); err != nil {
		rec.err = err
		return nil, err
	}

	c.version++
	c.expires = now.Add(req.TTL)
	c.ttl = req.TTL
	for _, name := range c.members {
		s.resources[name].expiresAt = c.expires
	}

	out := compositeView(c)
	rec.compositeOut = out
	return out, nil
}

// CompositeRelease releases the whole group together. A broken group cannot be
// "released" by the old holder; the diverged member stays under its new owner.
func (s *Service) CompositeRelease(req CompositeReleaseRequest) error {
	if req.CompositeID == "" || req.Holder == "" {
		return ErrInvalidRequest
	}
	content := requestContent{resources: []string{req.CompositeID}, holder: req.Holder}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "composite-release", content: content}
	rec, replayed, err := s.replayOrBegin(rec, "composite-release", req.RequestID, content)
	if err != nil {
		return err
	}
	if replayed {
		return rec.err
	}

	now := s.now()
	c, ok := s.composites[req.CompositeID]
	if !ok {
		rec.err = ErrCompositeNotFound
		return rec.err
	}
	if c.holder != req.Holder {
		rec.err = ErrHolderMismatch
		return rec.err
	}
	if err := groupIntact(s, c, now); err != nil {
		rec.err = err
		return err
	}

	c.released = true
	for _, name := range c.members {
		l := s.resources[name]
		l.lastHolder, l.lastToken = l.holder, l.fencingToken
		l.holder = ""
		l.compositeID = ""
		l.released = true
	}
	return nil
}

// CompositeWrite validates a protected write against one member. All members
// are checked at the current composite version, so an expired or taken-over
// sibling prevents further writes by the old group.
func (s *Service) CompositeWrite(req CompositeWriteRequest) error {
	if req.CompositeID == "" || req.Resource == "" || req.Holder == "" {
		return ErrInvalidRequest
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	c, ok := s.composites[req.CompositeID]
	if !ok {
		return ErrCompositeNotFound
	}
	if c.released {
		return ErrLeaseReleased
	}
	if c.holder != req.Holder {
		return ErrHolderMismatch
	}
	if err := groupIntact(s, c, now); err != nil {
		return err
	}
	token, ok := c.tokens[req.Resource]
	if !ok {
		return fmt.Errorf("%w: resource %q is not a member", ErrCompositeInvalid, req.Resource)
	}
	if token != req.FencingToken {
		return ErrFencingToken
	}
	return nil
}

func compositeView(c *composite) *CompositeLease {
	tokens := make(map[string]int64, len(c.tokens))
	for k, v := range c.tokens {
		tokens[k] = v
	}
	members := make([]string, len(c.members))
	copy(members, c.members)
	return &CompositeLease{
		ID:        c.id,
		Resources: members,
		Holder:    c.holder,
		Tokens:    tokens,
		Version:   c.version,
		ExpiresAt: c.expires,
	}
}
