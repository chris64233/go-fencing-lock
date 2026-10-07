package fencinglock

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func mustRead(t *testing.T, s *Service, res, holder string) *ReadLease {
	t.Helper()
	rl, err := s.AcquireRead(AcquireReadRequest{Resource: res, Holder: holder, TTL: time.Minute})
	if err != nil {
		t.Fatalf("acquire read %s/%s: %v", res, holder, err)
	}
	return rl
}

func TestSharedReadLeasesCoexist(t *testing.T) {
	s, _ := newTestService(t)
	r1 := mustRead(t, s, "r", "a")
	r2 := mustRead(t, s, "r", "b")
	if r1.Version == r2.Version {
		t.Fatalf("read versions must differ: %+v vs %+v", r1, r2)
	}
	if !r1.ExpiresAt.Equal(r2.ExpiresAt) {
		t.Fatalf("same ttl should give same deadline: %+v %+v", r1, r2)
	}

	info := s.QueryResource("r")
	if info.Status != StatusShared || len(info.Readers) != 2 {
		t.Fatalf("info = %+v", info)
	}

	// Read holders cannot perform protected writes.
	if err := s.Write(WriteRequest{Resource: "r", Holder: "a", FencingToken: 0}); !errors.Is(err, ErrLeaseReleased) {
		t.Fatalf("read holder write err = %v", err)
	}
	// A direct exclusive acquisition is blocked while readers are active.
	if _, err := s.Acquire(AcquireRequest{Resource: "r", Holder: "w", TTL: time.Minute}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("exclusive acquire err = %v", err)
	}
}

func TestWriteLeaseBlocksReadLease(t *testing.T) {
	s, _ := newTestService(t)
	w, err := s.Acquire(AcquireRequest{Resource: "r", Holder: "w", TTL: time.Minute})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := s.AcquireRead(AcquireReadRequest{Resource: "r", Holder: "a", TTL: time.Minute}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("read during write lease err = %v", err)
	}
	if err := s.Write(WriteRequest{Resource: "r", Holder: "w", FencingToken: w.FencingToken}); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestUpgradeWaitsForOtherReaders(t *testing.T) {
	s, clk := newTestService(t)
	mustRead(t, s, "r", "a")
	mustRead(t, s, "r", "b")

	rec, err := s.RequestUpgrade(RequestUpgradeRequest{Resource: "r", Holder: "a", UpgradeID: "u1"})
	if err != nil {
		t.Fatalf("request upgrade: %v", err)
	}
	if len(rec.FrozenReaders) != 2 {
		t.Fatalf("frozen readers = %v", rec.FrozenReaders)
	}

	// New read leases are blocked while the upgrade is pending.
	if _, err := s.AcquireRead(AcquireReadRequest{Resource: "r", Holder: "c", TTL: time.Minute}); !errors.Is(err, ErrUpgradePending) {
		t.Fatalf("read during upgrade err = %v", err)
	}
	// Non-applicant readers cannot renew past the freeze.
	if _, err := s.RenewRead(RenewReadRequest{Resource: "r", Holder: "b", TTL: time.Minute}); !errors.Is(err, ErrUpgradePending) {
		t.Fatalf("non-applicant renew err = %v", err)
	}
	// The applicant may renew without invalidating the upgrade.
	if _, err := s.RenewRead(RenewReadRequest{Resource: "r", Holder: "a", TTL: 2 * time.Minute}); err != nil {
		t.Fatalf("applicant renew: %v", err)
	}

	// b still holds: confirm must wait.
	if _, err := s.ConfirmUpgrade(ConfirmUpgradeRequest{
		Resource: "r", Holder: "a", UpgradeID: "u1", FrozenVersion: rec.FrozenVersion, TTL: time.Minute,
	}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("confirm while b active err = %v", err)
	}

	info := s.QueryResource("r")
	if info.PendingUpgrade == nil || fmt.Sprint(info.PendingUpgrade.WaitingOn) != "[b]" {
		t.Fatalf("pending upgrade = %+v", info.PendingUpgrade)
	}

	// Once b releases, the upgrade completes atomically.
	if err := s.ReleaseRead(ReleaseReadRequest{Resource: "r", Holder: "b"}); err != nil {
		t.Fatalf("release b: %v", err)
	}
	w, err := s.ConfirmUpgrade(ConfirmUpgradeRequest{
		Resource: "r", Holder: "a", UpgradeID: "u1", FrozenVersion: rec.FrozenVersion, TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if w.FencingToken != 1 || w.Holder != "a" {
		t.Fatalf("write lease = %+v", w)
	}
	if err := s.Write(WriteRequest{Resource: "r", Holder: "a", FencingToken: w.FencingToken}); err != nil {
		t.Fatalf("write after upgrade: %v", err)
	}
	info = s.QueryResource("r")
	if info.Status != StatusActive || len(info.Readers) != 0 || info.PendingUpgrade != nil {
		t.Fatalf("info after upgrade = %+v", info)
	}
	_ = clk
}

func TestUpgradeCompletesAfterOtherReaderExpires(t *testing.T) {
	s, clk := newTestService(t)
	mustRead(t, s, "r", "a")
	if _, err := s.AcquireRead(AcquireReadRequest{Resource: "r", Holder: "b", TTL: 30 * time.Second}); err != nil {
		t.Fatalf("read b: %v", err)
	}
	rec, err := s.RequestUpgrade(RequestUpgradeRequest{Resource: "r", Holder: "a", UpgradeID: "u1"})
	if err != nil {
		t.Fatalf("request upgrade: %v", err)
	}
	clk.advance(31 * time.Second) // b expires, a (1m) still active
	w, err := s.ConfirmUpgrade(ConfirmUpgradeRequest{
		Resource: "r", Holder: "a", UpgradeID: "u1", FrozenVersion: rec.FrozenVersion, TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("confirm after expiry: %v", err)
	}
	if w.Holder != "a" {
		t.Fatalf("write lease = %+v", w)
	}
}

func TestApplicantCannotReleaseThenGrab(t *testing.T) {
	s, _ := newTestService(t)
	mustRead(t, s, "r", "a")
	rec, err := s.RequestUpgrade(RequestUpgradeRequest{Resource: "r", Holder: "a", UpgradeID: "u1"})
	if err != nil {
		t.Fatalf("request upgrade: %v", err)
	}
	if err := s.ReleaseRead(ReleaseReadRequest{Resource: "r", Holder: "a"}); err != nil {
		t.Fatalf("release own read: %v", err)
	}
	// Confirm with no other readers must still fail: the applicant gave up
	// its read lease and cannot jump the queue.
	if _, err := s.ConfirmUpgrade(ConfirmUpgradeRequest{
		Resource: "r", Holder: "a", UpgradeID: "u1", FrozenVersion: rec.FrozenVersion, TTL: time.Minute,
	}); !errors.Is(err, ErrLeaseReleased) {
		t.Fatalf("confirm after self-release err = %v", err)
	}
	// A direct exclusive acquire is also blocked while the upgrade pends.
	if _, err := s.Acquire(AcquireRequest{Resource: "r", Holder: "a", TTL: time.Minute}); !errors.Is(err, ErrUpgradePending) {
		t.Fatalf("acquire during upgrade err = %v", err)
	}
}

func TestUpgradeIdempotencyAndConflicts(t *testing.T) {
	s, _ := newTestService(t)
	mustRead(t, s, "r", "a")
	rec, err := s.RequestUpgrade(RequestUpgradeRequest{Resource: "r", Holder: "a", UpgradeID: "u1"})
	if err != nil {
		t.Fatalf("request upgrade: %v", err)
	}
	// Same upgrade id, same content: original receipt returned.
	again, err := s.RequestUpgrade(RequestUpgradeRequest{Resource: "r", Holder: "a", UpgradeID: "u1"})
	if err != nil || again.FrozenVersion != rec.FrozenVersion {
		t.Fatalf("replay = %+v, %v", again, err)
	}
	// Same upgrade id, different holder: conflict.
	if _, err := s.RequestUpgrade(RequestUpgradeRequest{Resource: "r", Holder: "b", UpgradeID: "u1"}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflicting request err = %v", err)
	}
	// A second, different upgrade cannot start while one is pending.
	if _, err := s.RequestUpgrade(RequestUpgradeRequest{Resource: "r", Holder: "a", UpgradeID: "u2"}); !errors.Is(err, ErrUpgradePending) {
		t.Fatalf("second upgrade err = %v", err)
	}

	w, err := s.ConfirmUpgrade(ConfirmUpgradeRequest{
		Resource: "r", Holder: "a", UpgradeID: "u1", FrozenVersion: rec.FrozenVersion, TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	// Same upgrade id and content replays the original grant without
	// minting a new token.
	replay, err := s.ConfirmUpgrade(ConfirmUpgradeRequest{
		Resource: "r", Holder: "a", UpgradeID: "u1", FrozenVersion: rec.FrozenVersion, TTL: time.Minute,
	})
	if err != nil || replay.FencingToken != w.FencingToken {
		t.Fatalf("confirm replay = %+v, %v", replay, err)
	}
	// Same upgrade id but a changed frozen version conflicts and must not
	// overwrite the exclusive token.
	if _, err := s.ConfirmUpgrade(ConfirmUpgradeRequest{
		Resource: "r", Holder: "a", UpgradeID: "u1", FrozenVersion: rec.FrozenVersion + 1, TTL: time.Minute,
	}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed frozen version err = %v", err)
	}
	if info := s.QueryResource("r"); info.FencingToken != w.FencingToken || info.Holder != "a" {
		t.Fatalf("token overwritten: %+v", info)
	}
	// A stale receipt for an unknown upgrade id cannot mint a token either.
	if _, err := s.ConfirmUpgrade(ConfirmUpgradeRequest{
		Resource: "r", Holder: "a", UpgradeID: "u9", FrozenVersion: 0, TTL: time.Minute,
	}); !errors.Is(err, ErrUpgradeNotFound) {
		t.Fatalf("stale receipt err = %v", err)
	}
}

func TestCancelUpgradeKeepsReaders(t *testing.T) {
	s, clk := newTestService(t)
	ra := mustRead(t, s, "r", "a")
	mustRead(t, s, "r", "b")
	if _, err := s.RequestUpgrade(RequestUpgradeRequest{Resource: "r", Holder: "a", UpgradeID: "u1"}); err != nil {
		t.Fatalf("request upgrade: %v", err)
	}
	if err := s.CancelUpgrade(CancelUpgradeRequest{Resource: "r", Holder: "b", UpgradeID: "u1"}); !errors.Is(err, ErrHolderMismatch) {
		t.Fatalf("cancel by non-applicant err = %v", err)
	}
	if err := s.CancelUpgrade(CancelUpgradeRequest{Resource: "r", Holder: "a", UpgradeID: "u1"}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// Both readers survive the cancellation.
	info := s.QueryResource("r")
	if len(info.Readers) != 2 || info.PendingUpgrade != nil {
		t.Fatalf("info after cancel = %+v", info)
	}
	// New read leases are accepted again.
	mustRead(t, s, "r", "c")
	// The applicant's read lease still expires at its original deadline.
	clk.advance(time.Minute + time.Second)
	if _, err := s.RenewRead(RenewReadRequest{Resource: "r", Holder: "a", TTL: time.Minute}); !errors.Is(err, ErrLeaseReleased) {
		t.Fatalf("renew expired applicant read err = %v (original deadline %v)", err, ra.ExpiresAt)
	}
	// The cancelled upgrade can no longer be confirmed.
	if _, err := s.ConfirmUpgrade(ConfirmUpgradeRequest{
		Resource: "r", Holder: "a", UpgradeID: "u1", FrozenVersion: 0, TTL: time.Minute,
	}); !errors.Is(err, ErrRequestConflict) && !errors.Is(err, ErrUpgradeNotFound) {
		t.Fatalf("confirm cancelled upgrade err = %v", err)
	}
}

func TestReadReleaseRenewIdempotency(t *testing.T) {
	s, _ := newTestService(t)
	mustRead(t, s, "r", "a")
	rn, err := s.RenewRead(RenewReadRequest{Resource: "r", Holder: "a", TTL: 2 * time.Minute, RequestID: "rn-1"})
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	replay, err := s.RenewRead(RenewReadRequest{Resource: "r", Holder: "a", TTL: 2 * time.Minute, RequestID: "rn-1"})
	if err != nil || !replay.ExpiresAt.Equal(rn.ExpiresAt) {
		t.Fatalf("renew replay = %+v, %v", replay, err)
	}
	if _, err := s.RenewRead(RenewReadRequest{Resource: "r", Holder: "a", TTL: 3 * time.Minute, RequestID: "rn-1"}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("renew conflict err = %v", err)
	}
	if err := s.ReleaseRead(ReleaseReadRequest{Resource: "r", Holder: "a", RequestID: "rl-1"}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := s.ReleaseRead(ReleaseReadRequest{Resource: "r", Holder: "a", RequestID: "rl-1"}); err != nil {
		t.Fatalf("release replay: %v", err)
	}
}

func TestSharedReadConcurrentConsistency(t *testing.T) {
	s, _ := newTestService(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			holder := fmt.Sprintf("h%d", i)
			for j := 0; j < 50; j++ {
				_, _ = s.AcquireRead(AcquireReadRequest{Resource: "r", Holder: holder, TTL: time.Minute})
				_, _ = s.RequestUpgrade(RequestUpgradeRequest{Resource: "r", Holder: holder, UpgradeID: fmt.Sprintf("u-%d", i)})
				_, _ = s.ConfirmUpgrade(ConfirmUpgradeRequest{Resource: "r", Holder: holder, UpgradeID: fmt.Sprintf("u-%d", i), FrozenVersion: -1, TTL: time.Minute})
				_ = s.CancelUpgrade(CancelUpgradeRequest{Resource: "r", Holder: holder, UpgradeID: fmt.Sprintf("u-%d", i)})
				_ = s.ReleaseRead(ReleaseReadRequest{Resource: "r", Holder: holder})
				_ = s.ScanExpired()
				_ = s.QueryResources()
			}
		}(i)
	}
	wg.Wait()
	// The resource must show exactly one coherent ownership shape.
	info := s.QueryResource("r")
	if info.Holder != "" && len(info.Readers) > 0 {
		t.Fatalf("write lease and readers coexist: %+v", info)
	}
}
