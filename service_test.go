package fencinglock

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestService(t *testing.T) (*Service, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	return NewService(WithClock(clk.now)), clk
}

func TestNormalizeResourcesDedupAndSort(t *testing.T) {
	got := normalizeResources([]string{"c", "a", "b", "a", "c", "d"})
	want := []string{"a", "b", "c", "d"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("normalize = %v, want %v", got, want)
	}
}

func TestCompositeAcquireSucceedsAndReturnsTokens(t *testing.T) {
	s, _ := newTestService(t)
	cl, err := s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"r2", "r1", "r1"},
		Holder:    "h1",
		TTL:       time.Minute,
		RequestID: "req-1",
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if fmt.Sprint(cl.Resources) != "[r1 r2]" {
		t.Fatalf("members = %v", cl.Resources)
	}
	if cl.Tokens["r1"] != 1 || cl.Tokens["r2"] != 1 || cl.Version != 1 {
		t.Fatalf("tokens/version = %+v", cl)
	}

	for _, name := range cl.Resources {
		if err := s.CompositeWrite(CompositeWriteRequest{
			CompositeID:  cl.ID,
			Resource:     name,
			Holder:       "h1",
			FencingToken: cl.Tokens[name],
		}); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	info := s.QueryResource("r1")
	if info.Status != StatusComposite || info.CompositeID != cl.ID {
		t.Fatalf("r1 info = %+v", info)
	}
}

func TestCompositePartialAvailabilityRollsBack(t *testing.T) {
	s, clk := newTestService(t)

	if _, err := s.Acquire(AcquireRequest{Resource: "r2", Holder: "h-other", TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	r1, err := s.Acquire(AcquireRequest{Resource: "r1", Holder: "h-keep", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"r1", "r2", "r3"},
		Holder:    "h1",
		TTL:       time.Minute,
		RequestID: "req-group",
	})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}

	info := s.QueryResource("r1")
	if info.Status != StatusActive || info.Holder != "h-keep" || info.FencingToken != r1.FencingToken {
		t.Fatalf("r1 corrupted after rollback: %+v", info)
	}
	if err := s.Write(WriteRequest{Resource: "r1", Holder: "h-keep", FencingToken: r1.FencingToken}); err != nil {
		t.Fatalf("standalone lease broken after failed composite: %v", err)
	}
	if info := s.QueryResource("r2"); info.Holder != "h-other" || info.Status != StatusActive {
		t.Fatalf("r2 = %+v", info)
	}
	if info := s.QueryResource("r3"); info.Status != StatusFree {
		t.Fatalf("r3 = %+v", info)
	}

	clk.advance(time.Second)
	_, err = s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"r1", "r2", "r3"},
		Holder:    "h1",
		TTL:       time.Minute,
		RequestID: "req-group",
	})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("idempotent replay = %v", err)
	}
}

func TestIndependentFencingTokensAdvance(t *testing.T) {
	s, _ := newTestService(t)
	cl, err := s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"r1", "r2"}, Holder: "h1", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompositeRelease(CompositeReleaseRequest{CompositeID: cl.ID, Holder: "h1"}); err != nil {
		t.Fatal(err)
	}
	// Same holder reuses r1 standalone; r2 stays released. Then the same
	// holder forms a new group.
	if _, err := s.Acquire(AcquireRequest{Resource: "r1", Holder: "h2", TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	cl2, err := s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"r1", "r2"}, Holder: "h2", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cl2.Tokens["r1"] != 3 {
		t.Fatalf("r1 token = %d, want 3", cl2.Tokens["r1"])
	}
	if cl2.Tokens["r2"] != 2 {
		t.Fatalf("r2 token = %d, want 2", cl2.Tokens["r2"])
	}
}

func TestExpiredMemberTakeoverBreaksComposite(t *testing.T) {
	s, clk := newTestService(t)
	cl, err := s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"r1", "r2"}, Holder: "h1", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	clk.advance(2 * time.Minute)
	if got := s.ScanExpired(); len(got) != 2 {
		t.Fatalf("expired scan = %v", got)
	}

	ho, err := s.Handoff(HandoffRequest{Resource: "r1", From: "h1", To: "h2", TTL: time.Minute})
	if err != nil {
		t.Fatalf("handoff after expiry: %v", err)
	}
	if ho.FencingToken != cl.Tokens["r1"]+1 {
		t.Fatalf("new token = %d", ho.FencingToken)
	}

	err = s.CompositeWrite(CompositeWriteRequest{
		CompositeID: cl.ID, Resource: "r2", Holder: "h1", FencingToken: cl.Tokens["r2"],
	})
	if !errors.Is(err, ErrCompositeInvalid) {
		t.Fatalf("write sibling after takeover = %v", err)
	}
	err = s.CompositeWrite(CompositeWriteRequest{
		CompositeID: cl.ID, Resource: "r1", Holder: "h1", FencingToken: cl.Tokens["r1"],
	})
	if !errors.Is(err, ErrCompositeInvalid) && !errors.Is(err, ErrFencingToken) {
		t.Fatalf("write taken member = %v", err)
	}

	if _, err := s.CompositeRenew(CompositeRenewRequest{
		CompositeID: cl.ID, Holder: "h1", TTL: time.Minute,
	}); !errors.Is(err, ErrCompositeInvalid) {
		t.Fatalf("renew broken group = %v", err)
	}
	if err := s.CompositeRelease(CompositeReleaseRequest{
		CompositeID: cl.ID, Holder: "h1",
	}); !errors.Is(err, ErrCompositeInvalid) {
		t.Fatalf("release broken group = %v", err)
	}

	info, ok := s.QueryComposite(cl.ID)
	if !ok || info.Valid {
		t.Fatalf("composite query = %+v ok=%v", info, ok)
	}
	if info := s.QueryResource("r1"); info.Status != StatusActive || info.Holder != "h2" {
		t.Fatalf("r1 after takeover = %+v", info)
	}
}

func TestOldFencingTokenWriteRejected(t *testing.T) {
	s, _ := newTestService(t)
	l1, err := s.Acquire(AcquireRequest{Resource: "r", Holder: "h1", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	l2, err := s.Handoff(HandoffRequest{Resource: "r", From: "h1", To: "h2", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(WriteRequest{Resource: "r", Holder: "h1", FencingToken: l1.FencingToken}); !errors.Is(err, ErrFencingToken) && !errors.Is(err, ErrHolderMismatch) {
		t.Fatalf("old token write = %v", err)
	}
	if err := s.Write(WriteRequest{Resource: "r", Holder: "h2", FencingToken: l2.FencingToken}); err != nil {
		t.Fatalf("current token write: %v", err)
	}

	cl, err := s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"g1", "g2"}, Holder: "g", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	cl2, err := s.CompositeHandoff(CompositeHandoffRequest{
		CompositeID: cl.ID, From: "g", To: "g2", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = s.CompositeWrite(CompositeWriteRequest{
		CompositeID: cl.ID, Resource: "g1", Holder: "g", FencingToken: cl.Tokens["g1"],
	})
	if !errors.Is(err, ErrFencingToken) && !errors.Is(err, ErrHolderMismatch) {
		t.Fatalf("old composite token write = %v", err)
	}
	if err := s.CompositeWrite(CompositeWriteRequest{
		CompositeID: cl2.ID, Resource: "g2", Holder: "g2", FencingToken: cl2.Tokens["g2"],
	}); err != nil {
		t.Fatalf("new composite token write: %v", err)
	}
}

func TestIdempotentCompositeOpsAndConflicts(t *testing.T) {
	s, _ := newTestService(t)
	req := CompositeAcquireRequest{
		Resources: []string{"r1", "r2"}, Holder: "h1", TTL: time.Minute, RequestID: "R1",
	}
	cl, err := s.CompositeAcquire(req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.CompositeAcquire(req)
	if err != nil || again.ID != cl.ID || again.Version != cl.Version {
		t.Fatalf("idempotent acquire = %+v, %v", again, err)
	}

	_, err = s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"r1", "r2"}, Holder: "h2", TTL: time.Minute, RequestID: "R1",
	})
	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed holder = %v", err)
	}
	_, err = s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"r1"}, Holder: "h1", TTL: time.Minute, RequestID: "R1",
	})
	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed resources = %v", err)
	}
	_, err = s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"r1", "r2"}, Holder: "h1", TTL: 2 * time.Minute, RequestID: "R1",
	})
	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed ttl = %v", err)
	}

	renewReq := CompositeRenewRequest{CompositeID: cl.ID, Holder: "h1", TTL: time.Minute, RequestID: "R2"}
	r1, err := s.CompositeRenew(renewReq)
	if err != nil {
		t.Fatal(err)
	}
	if r1.Version != 2 {
		t.Fatalf("version after renew = %d", r1.Version)
	}
	r2, err := s.CompositeRenew(renewReq)
	if err != nil || r2.Version != 2 {
		t.Fatalf("idempotent renew = %+v, %v", r2, err)
	}

	relReq := CompositeReleaseRequest{CompositeID: cl.ID, Holder: "h1", RequestID: "R3"}
	if err := s.CompositeRelease(relReq); err != nil {
		t.Fatal(err)
	}
	if err := s.CompositeRelease(relReq); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
	if info := s.QueryResource("r1"); info.Status != StatusReleased {
		t.Fatalf("r1 after release = %+v", info)
	}
}

func TestSingleOpIdempotency(t *testing.T) {
	s, _ := newTestService(t)
	req := AcquireRequest{Resource: "r", Holder: "h", TTL: time.Minute, RequestID: "A1"}
	l1, err := s.Acquire(req)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := s.Acquire(req)
	if err != nil || l2.FencingToken != l1.FencingToken {
		t.Fatalf("idempotent acquire = %+v %v", l2, err)
	}
	if _, err := s.Acquire(AcquireRequest{Resource: "r", Holder: "x", TTL: time.Minute, RequestID: "A1"}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflict = %v", err)
	}

	rr := RenewRequest{Resource: "r", Holder: "h", TTL: time.Minute, RequestID: "A2"}
	if _, err := s.Renew(rr); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Renew(rr); err != nil {
		t.Fatalf("idempotent renew: %v", err)
	}
	lr := ReleaseRequest{Resource: "r", Holder: "h", RequestID: "A3"}
	if err := s.Release(lr); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(lr); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
}

func TestConcurrentAcquireHandoffScanNoDeadlock(t *testing.T) {
	s, _ := newTestService(t)
	resources := []string{"a", "b", "c", "d"}
	for _, r := range resources {
		if _, err := s.Acquire(AcquireRequest{Resource: r, Holder: "seed", TTL: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	run := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					fn()
				}
			}
		}()
	}

	run(func() {
		_, _ = s.CompositeAcquire(CompositeAcquireRequest{
			Resources: []string{"c", "a", "d", "b"}, Holder: "g", TTL: time.Millisecond,
		})
	})
	run(func() {
		_, _ = s.Acquire(AcquireRequest{Resource: "b", Holder: "solo", TTL: time.Hour})
	})
	run(func() {
		_, _ = s.Handoff(HandoffRequest{Resource: "a", From: "seed", To: "next", TTL: time.Hour})
		_, _ = s.Handoff(HandoffRequest{Resource: "a", From: "next", To: "seed", TTL: time.Hour})
	})
	run(func() { _ = s.ScanExpired() })
	run(func() {
		_ = s.Write(WriteRequest{Resource: "d", Holder: "seed", FencingToken: 1})
	})

	time.Sleep(300 * time.Millisecond)
	close(done)
	wg.Wait()
}

func TestConcurrentCompositeContendersOnlyOneWins(t *testing.T) {
	s, _ := newTestService(t)
	const n = 16
	var wg sync.WaitGroup
	wins := make(chan string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			holder := fmt.Sprintf("h-%d", i)
			cl, err := s.CompositeAcquire(CompositeAcquireRequest{
				Resources: []string{"x", "y"}, Holder: holder, TTL: time.Minute,
			})
			if err == nil {
				wins <- cl.Holder
			}
		}(i)
	}
	wg.Wait()
	close(wins)
	count := 0
	for range wins {
		count++
	}
	if count != 1 {
		t.Fatalf("winners = %d, want exactly 1", count)
	}
}

func TestInvalidRequests(t *testing.T) {
	s, _ := newTestService(t)
	if _, err := s.CompositeAcquire(CompositeAcquireRequest{
		Resources: nil, Holder: "h", TTL: time.Minute,
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("empty resources = %v", err)
	}
	if _, err := s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"r"}, Holder: "", TTL: time.Minute,
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("empty holder = %v", err)
	}
	if _, err := s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"r"}, Holder: "h", TTL: 0,
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("zero ttl = %v", err)
	}
	if _, err := s.Acquire(AcquireRequest{}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("bad acquire = %v", err)
	}
}

func TestCompositeRenewExtendsWholeGroup(t *testing.T) {
	s, clk := newTestService(t)
	cl, err := s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"r1", "r2"}, Holder: "h", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	clk.advance(50 * time.Second)
	r2, err := s.CompositeRenew(CompositeRenewRequest{
		CompositeID: cl.ID, Holder: "h", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if r2.Version != 2 {
		t.Fatalf("version = %d", r2.Version)
	}
	clk.advance(30 * time.Second)
	// Original deadline would have passed; renewal keeps writes valid.
	if err := s.CompositeWrite(CompositeWriteRequest{
		CompositeID: cl.ID, Resource: "r1", Holder: "h", FencingToken: cl.Tokens["r1"],
	}); err != nil {
		t.Fatalf("write after renew: %v", err)
	}
	// Wrong holder cannot renew.
	if _, err := s.CompositeRenew(CompositeRenewRequest{
		CompositeID: cl.ID, Holder: "intruder", TTL: time.Minute,
	}); !errors.Is(err, ErrHolderMismatch) {
		t.Fatalf("renew by stranger = %v", err)
	}
}

func TestCompositeReleaseIsAtomic(t *testing.T) {
	s, _ := newTestService(t)
	cl, err := s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"r1", "r2", "r3"}, Holder: "h", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompositeRelease(CompositeReleaseRequest{CompositeID: cl.ID, Holder: "h"}); err != nil {
		t.Fatal(err)
	}
	for _, name := range cl.Resources {
		info := s.QueryResource(name)
		if info.Status != StatusReleased {
			t.Fatalf("%s = %+v, want released", name, info)
		}
		if info.CompositeID != "" {
			t.Fatalf("%s still bound to composite %q", name, info.CompositeID)
		}
	}
	if _, ok := s.QueryComposite(cl.ID); !ok {
		t.Fatal("composite record dropped")
	}
}

func TestSingleAcquireCannotStealIntactGroupMember(t *testing.T) {
	s, _ := newTestService(t)
	cl, err := s.CompositeAcquire(CompositeAcquireRequest{
		Resources: []string{"r1", "r2"}, Holder: "h", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire(AcquireRequest{Resource: "r1", Holder: "other", TTL: time.Minute}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("steal member = %v", err)
	}
	// Group still works.
	if err := s.CompositeWrite(CompositeWriteRequest{
		CompositeID: cl.ID, Resource: "r2", Holder: "h", FencingToken: cl.Tokens["r2"],
	}); err != nil {
		t.Fatalf("group broken: %v", err)
	}
}
