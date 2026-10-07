package fencinglock

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func mustRead(t *testing.T, s *Service, resource, holder string) *ReadLease {
	t.Helper()
	rl, err := s.AcquireRead(AcquireReadRequest{
		Resource: resource, Holder: holder, TTL: time.Minute,
		RequestID: "rd-" + resource + "-" + holder,
	})
	if err != nil {
		t.Fatalf("AcquireRead %s/%s: %v", resource, holder, err)
	}
	return rl
}

func TestSharedReadCoexistsAndBlocksExclusive(t *testing.T) {
	s, _ := newTestService(t)
	r1 := mustRead(t, s, "res", "a")
	r2 := mustRead(t, s, "res", "b")
	if r1.Holder != "a" || r2.Holder != "b" || r1.ExpiresAt.IsZero() {
		t.Fatalf("read leases = %+v %+v", r1, r2)
	}
	if r1.Version == 0 || r2.Version == 0 {
		t.Fatalf("read leases must record a version: %+v %+v", r1, r2)
	}

	if _, err := s.Acquire(AcquireRequest{Resource: "res", Holder: "w", TTL: time.Minute}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("exclusive acquire over readers = %v", err)
	}
	if _, err := s.Handoff(HandoffRequest{Resource: "res", From: "a", To: "w", TTL: time.Minute}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("handoff over readers = %v", err)
	}
	if _, err := s.CompositeAcquire(CompositeAcquireRequest{Resources: []string{"res"}, Holder: "w", TTL: time.Minute}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("composite acquire over readers = %v", err)
	}
	// Readers must not perform protected writes.
	if err := s.Write(WriteRequest{Resource: "res", Holder: "a", FencingToken: 0}); !errors.Is(err, ErrLeaseReleased) {
		t.Fatalf("write by reader = %v", err)
	}

	info := s.QueryResource("res")
	if info.Status != StatusShared || len(info.Readers) != 2 {
		t.Fatalf("info = %+v", info)
	}
}

func TestWriteLeaseBlocksReadLease(t *testing.T) {
	s, _ := newTestService(t)
	w, err := s.Acquire(AcquireRequest{Resource: "res", Holder: "w", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcquireRead(AcquireReadRequest{Resource: "res", Holder: "a", TTL: time.Minute}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("read under write lease = %v", err)
	}
	if err := s.Write(WriteRequest{Resource: "res", Holder: "w", FencingToken: w.FencingToken}); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestUpgradeWaitsForOtherReaders(t *testing.T) {
	s, clk := newTestService(t)
	mustRead(t, s, "res", "a")
	mustRead(t, s, "res", "b")

	// The applicant keeps its read lease alive past the other reader's TTL.
	if _, err := s.RenewRead(RenewReadRequest{Resource: "res", Holder: "a", TTL: 5 * time.Minute}); err != nil {
		t.Fatalf("applicant renew: %v", err)
	}

	grant, err := s.RequestUpgrade(UpgradeRequest{Resource: "res", Holder: "a", RequestID: "up-1"})
	if err != nil {
		t.Fatalf("request upgrade: %v", err)
	}
	if fmt.Sprint(grant.FrozenReaders) != "[a b]" {
		t.Fatalf("frozen readers = %v", grant.FrozenReaders)
	}

	// New reads are blocked while the upgrade waits.
	if _, err := s.AcquireRead(AcquireReadRequest{Resource: "res", Holder: "c", TTL: time.Minute}); !errors.Is(err, ErrUpgradePending) {
		t.Fatalf("read during pending upgrade = %v", err)
	}
	// So are exclusive acquisitions.
	if _, err := s.Acquire(AcquireRequest{Resource: "res", Holder: "c", TTL: time.Minute}); !errors.Is(err, ErrUpgradePending) {
		t.Fatalf("acquire during pending upgrade = %v", err)
	}

	// b still holds: completion must refuse.
	if _, err := s.CompleteUpgrade(CompleteUpgradeRequest{
		Resource: "res", Holder: "a", UpgradeID: "up-1",
		FrozenVersion: grant.FrozenVersion, TTL: time.Minute,
	}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("complete with reader b active = %v", err)
	}

	// b expires rather than releases; completion then succeeds atomically.
	clk.advance(2 * time.Minute)
	w, err := s.CompleteUpgrade(CompleteUpgradeRequest{
		Resource: "res", Holder: "a", UpgradeID: "up-1",
		FrozenVersion: grant.FrozenVersion, TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("complete upgrade: %v", err)
	}
	if w.FencingToken != 1 || w.Holder != "a" {
		t.Fatalf("write lease = %+v", w)
	}
	if err := s.Write(WriteRequest{Resource: "res", Holder: "a", FencingToken: w.FencingToken}); err != nil {
		t.Fatalf("write after upgrade: %v", err)
	}
	info := s.QueryResource("res")
	if info.Status != StatusActive || len(info.Readers) != 0 || info.Upgrade != nil {
		t.Fatalf("info after upgrade = %+v", info)
	}
}

func TestUpgradeApplicantCannotReleaseAndPreempt(t *testing.T) {
	s, _ := newTestService(t)
	mustRead(t, s, "res", "a")
	grant, err := s.RequestUpgrade(UpgradeRequest{Resource: "res", Holder: "a", RequestID: "up-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseRead(ReleaseReadRequest{Resource: "res", Holder: "a"}); err != nil {
		t.Fatal(err)
	}
	// The applicant released its read lease: completion must fail, and the
	// pending upgrade still blocks a direct exclusive grab.
	if _, err := s.CompleteUpgrade(CompleteUpgradeRequest{
		Resource: "res", Holder: "a", UpgradeID: "up-1",
		FrozenVersion: grant.FrozenVersion, TTL: time.Minute,
	}); !errors.Is(err, ErrNoReadLease) {
		t.Fatalf("complete after self-release = %v", err)
	}
	if _, err := s.Acquire(AcquireRequest{Resource: "res", Holder: "a", TTL: time.Minute}); !errors.Is(err, ErrUpgradePending) {
		t.Fatalf("preempt after self-release = %v", err)
	}
}

func TestStaleUpgradeReceiptCannotWrite(t *testing.T) {
	s, _ := newTestService(t)
	mustRead(t, s, "res", "a")
	old, err := s.RequestUpgrade(UpgradeRequest{Resource: "res", Holder: "a", RequestID: "up-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CancelUpgrade(CancelUpgradeRequest{Resource: "res", Holder: "a", UpgradeID: "up-1"}); err != nil {
		t.Fatal(err)
	}
	fresh, err := s.RequestUpgrade(UpgradeRequest{Resource: "res", Holder: "a", RequestID: "up-2"})
	if err != nil {
		t.Fatal(err)
	}
	if fresh.FrozenVersion == old.FrozenVersion {
		t.Fatalf("frozen version did not advance: %d", fresh.FrozenVersion)
	}

	// The old receipt and old read version must not produce a write lease.
	if _, err := s.CompleteUpgrade(CompleteUpgradeRequest{
		Resource: "res", Holder: "a", UpgradeID: "up-1",
		FrozenVersion: old.FrozenVersion, TTL: time.Minute,
	}); !errors.Is(err, ErrUpgradeConflict) {
		t.Fatalf("stale receipt = %v", err)
	}
	if _, err := s.CompleteUpgrade(CompleteUpgradeRequest{
		Resource: "res", Holder: "a", UpgradeID: "up-2",
		FrozenVersion: old.FrozenVersion, TTL: time.Minute,
	}); !errors.Is(err, ErrUpgradeConflict) {
		t.Fatalf("stale read version = %v", err)
	}
	if info := s.QueryResource("res"); info.FencingToken != 0 {
		t.Fatalf("token advanced by stale receipt: %+v", info)
	}

	w, err := s.CompleteUpgrade(CompleteUpgradeRequest{
		Resource: "res", Holder: "a", UpgradeID: "up-2",
		FrozenVersion: fresh.FrozenVersion, TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("complete fresh upgrade: %v", err)
	}
	if w.FencingToken != 1 {
		t.Fatalf("token = %d, want 1", w.FencingToken)
	}
	// A replayed stale completion must not overwrite the new token.
	if _, err := s.CompleteUpgrade(CompleteUpgradeRequest{
		Resource: "res", Holder: "a", UpgradeID: "up-2",
		FrozenVersion: fresh.FrozenVersion, TTL: time.Minute, RequestID: "retry",
	}); !errors.Is(err, ErrUpgradeNotFound) {
		t.Fatalf("replayed completion = %v", err)
	}
	if info := s.QueryResource("res"); info.FencingToken != 1 || info.Holder != "a" {
		t.Fatalf("token overwritten: %+v", info)
	}
}

func TestCancelUpgradeKeepsReaders(t *testing.T) {
	s, clk := newTestService(t)
	mustRead(t, s, "res", "a")
	mustRead(t, s, "res", "b")
	if _, err := s.RequestUpgrade(UpgradeRequest{Resource: "res", Holder: "a", RequestID: "up-1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CancelUpgrade(CancelUpgradeRequest{Resource: "res", Holder: "a", UpgradeID: "up-1"}); err != nil {
		t.Fatal(err)
	}

	info := s.QueryResource("res")
	if len(info.Readers) != 2 || info.Upgrade != nil {
		t.Fatalf("readers lost on cancel: %+v", info)
	}
	// Applicant's read lease is still valid under its original deadline.
	if _, err := s.RenewRead(RenewReadRequest{Resource: "res", Holder: "a", TTL: time.Minute}); err != nil {
		t.Fatalf("applicant read after cancel: %v", err)
	}
	// New reads are accepted again.
	mustRead(t, s, "res", "c")
	// Original deadlines still apply: after a minute the un-renewed b lapses.
	clk.advance(2 * time.Minute)
	if err := s.ReleaseRead(ReleaseReadRequest{Resource: "res", Holder: "b"}); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("release expired reader = %v", err)
	}
}

func TestUpgradeIdempotency(t *testing.T) {
	s, _ := newTestService(t)
	mustRead(t, s, "res", "a")
	g1, err := s.RequestUpgrade(UpgradeRequest{Resource: "res", Holder: "a", RequestID: "up-1"})
	if err != nil {
		t.Fatal(err)
	}
	g2, err := s.RequestUpgrade(UpgradeRequest{Resource: "res", Holder: "a", RequestID: "up-1"})
	if err != nil {
		t.Fatalf("same upgrade number replay: %v", err)
	}
	if g1.FrozenVersion != g2.FrozenVersion || g1.UpgradeID != g2.UpgradeID {
		t.Fatalf("replay mismatch: %+v vs %+v", g1, g2)
	}
	// Same number, different holder: conflict.
	if _, err := s.RequestUpgrade(UpgradeRequest{Resource: "res", Holder: "b", RequestID: "up-1"}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflicting content = %v", err)
	}
	// Different number while one is pending: conflict.
	if _, err := s.RequestUpgrade(UpgradeRequest{Resource: "res", Holder: "a", RequestID: "up-2"}); !errors.Is(err, ErrUpgradeConflict) {
		t.Fatalf("second pending upgrade = %v", err)
	}
}

func TestUpgradeConcurrentDrainProducesSingleOwner(t *testing.T) {
	s, _ := newTestService(t)
	mustRead(t, s, "res", "a")
	mustRead(t, s, "res", "b")
	grant, err := s.RequestUpgrade(UpgradeRequest{Resource: "res", Holder: "a", RequestID: "up-1"})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch i % 4 {
			case 0:
				_ = s.ReleaseRead(ReleaseReadRequest{Resource: "res", Holder: "b"})
			case 1:
				_, _ = s.RenewRead(RenewReadRequest{Resource: "res", Holder: "a", TTL: time.Minute})
			case 2:
				_ = s.ScanExpired()
			case 3:
				_, _ = s.CompleteUpgrade(CompleteUpgradeRequest{
					Resource: "res", Holder: "a", UpgradeID: "up-1",
					FrozenVersion: grant.FrozenVersion, TTL: time.Minute,
				})
			}
		}(i)
	}
	wg.Wait()

	info := s.QueryResource("res")
	// Exactly one coherent ownership: either readers with a pending upgrade,
	// or a single exclusive holder. Never both.
	if info.Upgrade != nil && info.Holder != "" {
		t.Fatalf("upgrade still pending after write grant: %+v", info)
	}
	if info.Holder != "" && len(info.Readers) != 0 {
		t.Fatalf("readers coexist with exclusive holder: %+v", info)
	}
}

func TestQueryShowsUpgradeDetails(t *testing.T) {
	s, _ := newTestService(t)
	mustRead(t, s, "res", "a")
	mustRead(t, s, "res", "b")
	grant, err := s.RequestUpgrade(UpgradeRequest{Resource: "res", Holder: "a", RequestID: "up-1"})
	if err != nil {
		t.Fatal(err)
	}
	info := s.QueryResource("res")
	if info.Upgrade == nil {
		t.Fatalf("no upgrade in query: %+v", info)
	}
	if info.Upgrade.UpgradeID != "up-1" || info.Upgrade.Applicant != "a" ||
		info.Upgrade.FrozenVersion != grant.FrozenVersion {
		t.Fatalf("upgrade info = %+v", info.Upgrade)
	}
	if fmt.Sprint(info.Upgrade.WaitingOn) != "[b]" {
		t.Fatalf("waiting on = %v", info.Upgrade.WaitingOn)
	}
	if info.Reason == "" {
		t.Fatal("expected a blocking reason")
	}
}
