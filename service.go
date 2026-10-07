package fencinglock

import (
	"sort"
	"sync"
	"time"
)

// lease is the per-resource lease state.
type lease struct {
	fencingToken int64
	holder       string
	expiresAt    time.Time

	// compositeID identifies the composite lease that currently owns this
	// resource. It is empty when the resource is held by a standalone lease
	// or carries no active lease.
	compositeID string

	// released indicates the most recent lease was explicitly released and
	// no new lease has been granted yet. It is cleared on the next grant.
	released bool

	// lastHolder/lastToken retain the previous grant metadata so queries can
	// explain why an expired or released lease is no longer usable.
	lastHolder string
	lastToken  int64
}

func (l *lease) active(now time.Time) bool {
	return l.holder != "" && !l.expiresAt.Before(now)
}

// expired treats a lease with a holder whose deadline has passed as expired.
func (l *lease) expired(now time.Time) bool {
	return l.holder != "" && l.expiresAt.Before(now)
}

// composite is the recorded state of a composite lease.
type composite struct {
	id       string
	holder   string
	ttl      time.Duration
	expires  time.Time
	version  int64
	members  []string
	tokens   map[string]int64
	released bool
}

// idemRecord stores the original request content and outcome of an idempotent
// operation keyed by request number.
type idemRecord struct {
	kind    string
	content requestContent
	err     error

	leaseOut     *Lease
	compositeOut *CompositeLease
}

// requestContent is the stable fingerprint of an idempotent request.
type requestContent struct {
	resources []string
	holder    string
	ttl       time.Duration
	from      string
}

func sameResources(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Service is the lease service. A single mutex guards all state; resources are
// always processed in sorted order, which makes deadlocks impossible even when
// single acquires, handoffs, expiry scans and composite acquisitions race.
type Service struct {
	mu         sync.Mutex
	resources  map[string]*lease
	composites map[string]*composite
	idems      map[string]*idemRecord
	nowFunc    func() time.Time
}

// Option configures a Service.
type Option func(*Service)

// WithClock injects a clock, mainly useful in tests.
func WithClock(nowFunc func() time.Time) Option {
	return func(s *Service) { s.nowFunc = nowFunc }
}

// NewService creates a lease service.
func NewService(opts ...Option) *Service {
	s := &Service{
		resources:  make(map[string]*lease),
		composites: make(map[string]*composite),
		idems:      make(map[string]*idemRecord),
		nowFunc:    time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Service) now() time.Time { return s.nowFunc() }

// normalizeResources de-duplicates resource ids and returns them in the
// stable order used for every multi-resource operation.
func normalizeResources(resources []string) []string {
	seen := make(map[string]struct{}, len(resources))
	out := make([]string, 0, len(resources))
	for _, r := range resources {
		if _, ok := seen[r]; ok {
			continue
		}
		seen[r] = struct{}{}
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

func (s *Service) getOrCreateResource(name string) *lease {
	l, ok := s.resources[name]
	if !ok {
		l = &lease{}
		s.resources[name] = l
	}
	return l
}

// snapshot copies every resource touched by a composite acquisition so the
// whole group can be restored atomically on failure.
func snapshot(resources map[string]*lease) map[string]*lease {
	cp := make(map[string]*lease, len(resources))
	for name, l := range resources {
		clone := *l
		cp[name] = &clone
	}
	return cp
}

func (s *Service) restore(snap map[string]*lease) {
	for name, l := range snap {
		clone := *l
		s.resources[name] = &clone
	}
}

func (s *Service) replayOrBegin(rec *idemRecord, kind, reqID string, content requestContent) (*idemRecord, bool, error) {
	if reqID == "" {
		return rec, false, nil
	}
	if existing, ok := s.idems[reqID]; ok {
		if existing.kind != kind ||
			!sameResources(existing.content.resources, content.resources) ||
			existing.content.holder != content.holder ||
			existing.content.ttl != content.ttl ||
			existing.content.from != content.from {
			return existing, true, ErrRequestConflict
		}
		return existing, true, nil
	}
	s.idems[reqID] = rec
	return rec, false, nil
}
