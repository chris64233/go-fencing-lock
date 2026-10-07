package fencinglock

import "time"

// Status describes the current usability of a resource.
type Status string

const (
	// StatusActive is an active standalone lease.
	StatusActive Status = "active"
	// StatusComposite is an active lease owned by a composite lease.
	StatusComposite Status = "composite"
	// StatusExpired is a lease whose deadline has passed.
	StatusExpired Status = "expired"
	// StatusReleased is a lease that was explicitly released.
	StatusReleased Status = "released"
	// StatusShared is a resource carrying one or more shared read leases
	// and no exclusive write lease.
	StatusShared Status = "shared"
	// StatusFree has never carried a lease.
	StatusFree Status = "free"
)

// ReadLeaseInfo describes one shared read lease on a resource.
type ReadLeaseInfo struct {
	Holder    string
	Version   int64
	ExpiresAt time.Time
	Expired   bool
}

// UpgradeInfo describes a pending read-to-write upgrade.
type UpgradeInfo struct {
	UpgradeID     string
	Applicant     string
	FrozenVersion int64
	FrozenReaders []string
	// WaitingOn lists the frozen readers (besides the applicant) whose
	// leases are still active and block completion.
	WaitingOn []string
}

// ResourceInfo explains the current state of one resource.
type ResourceInfo struct {
	Resource       string
	Status         Status
	FencingToken   int64
	Holder         string
	CompositeID    string
	ExpiresAt      time.Time
	Readers        []ReadLeaseInfo
	PendingUpgrade *UpgradeInfo
	Reason         string
}

// CompositeInfo describes a composite lease and its member status.
type CompositeInfo struct {
	ID        string
	Holder    string
	Version   int64
	ExpiresAt time.Time
	Released  bool
	Members   []ResourceInfo
	Valid     bool
	Reason    string
}

// QueryResource returns the current token, holder, composite relation and the
// reason the lease is (or is not) usable.
func (s *Service) QueryResource(name string) ResourceInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	l, ok := s.resources[name]
	if !ok {
		return ResourceInfo{Resource: name, Status: StatusFree, Reason: "no lease has ever been granted"}
	}
	return s.describe(name, l, now)
}

// QueryResources returns sorted resource information for every known resource.
func (s *Service) QueryResources() []ResourceInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	names := make([]string, 0, len(s.resources))
	for name := range s.resources {
		names = append(names, name)
	}
	sortStrings(names)
	out := make([]ResourceInfo, 0, len(names))
	for _, name := range names {
		out = append(out, s.describe(name, s.resources[name], now))
	}
	return out
}

// QueryComposite returns a composite lease together with each member's state.
func (s *Service) QueryComposite(id string) (CompositeInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	c, ok := s.composites[id]
	if !ok {
		return CompositeInfo{}, false
	}
	info := CompositeInfo{
		ID:        c.id,
		Holder:    c.holder,
		Version:   c.version,
		ExpiresAt: c.expires,
		Released:  c.released,
		Valid:     true,
	}
	for _, name := range c.members {
		ri := s.describe(name, s.resources[name], now)
		info.Members = append(info.Members, ri)
	}
	if c.released {
		info.Valid = false
		info.Reason = "composite lease was released"
	} else if err := groupIntact(s, c, now); err != nil {
		info.Valid = false
		info.Reason = err.Error()
	}
	return info, true
}

func (s *Service) describe(name string, l *lease, now time.Time) ResourceInfo {
	info := ResourceInfo{
		Resource:     name,
		FencingToken: l.fencingToken,
		Holder:       l.holder,
		CompositeID:  l.compositeID,
		ExpiresAt:    l.expiresAt,
	}
	for h, r := range l.readers {
		info.Readers = append(info.Readers, ReadLeaseInfo{
			Holder:    h,
			Version:   r.version,
			ExpiresAt: r.expiresAt,
			Expired:   !r.active(now),
		})
	}
	sortSlice(info.Readers)
	if u := l.upgrade; u != nil {
		pu := &UpgradeInfo{
			UpgradeID:     u.id,
			Applicant:     u.applicant,
			FrozenVersion: u.frozenVersion,
		}
		for h := range u.readers {
			pu.FrozenReaders = append(pu.FrozenReaders, h)
			if h == u.applicant {
				continue
			}
			if r, ok := l.readers[h]; ok && r.active(now) {
				pu.WaitingOn = append(pu.WaitingOn, h)
			}
		}
		sortStrings(pu.FrozenReaders)
		sortStrings(pu.WaitingOn)
		info.PendingUpgrade = pu
	}
	switch {
	case l.holder != "" && l.expired(now):
		info.Status = StatusExpired
		info.Reason = "lease expired at " + l.expiresAt.UTC().Format(time.RFC3339Nano)
	case l.released:
		info.Status = StatusReleased
		token := int64(0)
		holder := ""
		if l.lastToken != 0 {
			token, holder = l.lastToken, l.lastHolder
		}
		info.FencingToken = token
		info.Holder = holder
		info.Reason = "lease was explicitly released"
	case l.holder == "":
		if len(info.Readers) > 0 {
			info.Status = StatusShared
			info.Reason = "shared read leases active"
			if info.PendingUpgrade != nil {
				info.Reason = "shared read leases active; upgrade " +
					info.PendingUpgrade.UpgradeID + " pending, new read leases blocked"
			}
			break
		}
		info.Status = StatusFree
		info.Reason = "no active lease"
	case l.compositeID != "":
		info.Status = StatusComposite
		c := s.composites[l.compositeID]
		if c == nil {
			info.Reason = "composite lease record missing"
		} else if err := groupIntact(s, c, now); err != nil {
			info.Reason = err.Error()
		} else {
			info.Reason = "held by composite lease " + l.compositeID
		}
	default:
		info.Status = StatusActive
		info.Reason = "active standalone lease"
	}
	if info.PendingUpgrade != nil && info.Status != StatusShared {
		info.Reason += "; upgrade " + info.PendingUpgrade.UpgradeID + " pending"
	}
	return info
}

func sortSlice(a []ReadLeaseInfo) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j-1].Holder > a[j].Holder; j-- {
			a[j-1], a[j] = a[j], a[j-1]
		}
	}
}

func sortStrings(a []string) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j-1] > a[j]; j-- {
			a[j-1], a[j] = a[j], a[j-1]
		}
	}
}

// ScanExpired returns the names of resources whose leases are past their
// deadline. Scanning is advisory: expiry is also enforced lazily on every
// operation, and scanning never mutates state, so it cannot race with
// acquisitions or releases.
func (s *Service) ScanExpired() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var out []string
	for name, l := range s.resources {
		if l.holder != "" && l.expired(now) {
			out = append(out, name)
		}
	}
	sortStrings(out)
	return out
}
