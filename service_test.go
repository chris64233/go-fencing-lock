package fencinglock

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func newTestService() (*Service, *fakeClock) {
	clk := &fakeClock{t: time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)}
	s := NewService()
	s.SetClock(func() time.Time { return clk.t })
	return s, clk
}

func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func TestAcquireFencingTokensIncrease(t *testing.T) {
	s, _ := newTestService()

	l1, err := s.Acquire(AcquireRequest{ResourceID: "r1", Holder: "alice", TTL: time.Minute})
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if err := s.Release(ReleaseRequest{ResourceID: "r1", Holder: "alice"}); err != nil {
		t.Fatalf("release: %v", err)
	}
	l2, err := s.Acquire(AcquireRequest{ResourceID: "r1", Holder: "bob", TTL: time.Minute})
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if l2.Token <= l1.Token {
		t.Fatalf("token must increase after release: %d -> %d", l1.Token, l2.Token)
	}
}

func TestTransferIssuesNewFencingToken(t *testing.T) {
	s, clk := newTestService()
	l, err := s.Acquire(AcquireRequest{ResourceID: "r1", Holder: "alice", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(WriteRequest{ResourceID: "r1", Holder: "alice", Token: l.Token}); err != nil {
		t.Fatalf("write with current token: %v", err)
	}

	t2, err := s.Transfer(TransferRequest{ResourceID: "r1", From: "alice", To: "carol", TTL: time.Minute})
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if t2.Token <= l.Token {
		t.Fatalf("transfer must raise token: %d -> %d", l.Token, t2.Token)
	}
	if err := s.Write(WriteRequest{ResourceID: "r1", Holder: "alice", Token: l.Token}); !errors.Is(err, ErrNotHolder) {
		t.Fatalf("old holder write = %v, want ErrNotHolder", err)
	}
	if err := s.Write(WriteRequest{ResourceID: "r1", Holder: "carol", Token: l.Token}); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("stale token write = %v, want ErrInvalidToken", err)
	}
	if err := s.Write(WriteRequest{ResourceID: "r1", Holder: "carol", Token: t2.Token}); err != nil {
		t.Fatalf("new holder write: %v", err)
	}

	clk.advance(time.Minute)
	if _, err := s.Transfer(TransferRequest{ResourceID: "r1", From: "carol", To: "dave", TTL: time.Minute}); !errors.Is(err, ErrNotHolder) {
		t.Fatalf("transfer after expiry = %v, want ErrNotHolder", err)
	}
}

func TestGroupAcquireNormalizesResourceOrder(t *testing.T) {
	s, _ := newTestService()
	// 故意乱序并带重复编号。
	g, err := s.AcquireGroup(GroupRequest{
		ResourceIDs: []string{"r3", "r1", "r2", "r1"},
		Holder:      "worker-a",
		TTL:         time.Minute,
		RequestID:   "g-1",
	})
	if err != nil {
		t.Fatalf("acquire group: %v", err)
	}
	want := []string{"r1", "r2", "r3"}
	if fmt.Sprint(g.Resources) != fmt.Sprint(want) {
		t.Fatalf("normalized resources = %v, want %v", g.Resources, want)
	}
	if len(g.Tokens) != 3 {
		t.Fatalf("tokens = %v, want 3 entries", g.Tokens)
	}

	// 每个资源的 token 在重新申请后必须独立递增。
	old := map[string]int64{}
	for _, id := range want {
		old[id] = g.Tokens[id]
	}
	if err := s.ReleaseGroup(GroupOpRequest{GroupID: g.GroupID, Holder: "worker-a"}); err != nil {
		t.Fatal(err)
	}
	g2, err := s.AcquireGroup(GroupRequest{
		ResourceIDs: []string{"r1", "r2", "r3"},
		Holder:      "worker-a",
		TTL:         time.Minute,
		RequestID:   "g-2",
	})
	if err != nil {
		t.Fatalf("re-acquire group: %v", err)
	}
	for _, id := range want {
		if g2.Tokens[id] <= old[id] {
			t.Fatalf("token for %s did not increase: %d -> %d", id, old[id], g2.Tokens[id])
		}
	}
}

func TestGroupPartialAvailabilityRollsBack(t *testing.T) {
	s, _ := newTestService()

	// r1、r3 上已有 alice 的单资源租约，r2 被 bob 占住。
	l1, err := s.Acquire(AcquireRequest{ResourceID: "r1", Holder: "alice", TTL: time.Minute, RequestID: "a1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire(AcquireRequest{ResourceID: "r2", Holder: "bob", TTL: time.Minute, RequestID: "a2"}); err != nil {
		t.Fatal(err)
	}
	l3, err := s.Acquire(AcquireRequest{ResourceID: "r3", Holder: "alice", TTL: time.Minute, RequestID: "a3"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.AcquireGroup(GroupRequest{
		ResourceIDs: []string{"r1", "r2", "r3"},
		Holder:      "alice",
		TTL:         time.Minute,
		RequestID:   "g-fail",
	})
	var unavailable *UnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("group acquire = %v, want *UnavailableError", err)
	}
	if unavailable.Resource != "r2" {
		t.Fatalf("blocked resource = %q, want r2", unavailable.Resource)
	}

	// 回滚后：r1/r3 仍是 alice 的有效单资源租约，token 与持有者不变。
	info1 := s.GetResource("r1")
	if !info1.Active || info1.Holder != "alice" || info1.Token != l1.Token || info1.GroupID != 0 {
		t.Fatalf("r1 mutated after failed group acquire: %+v", info1)
	}
	info3 := s.GetResource("r3")
	if !info3.Active || info3.Holder != "alice" || info3.Token != l3.Token || info3.GroupID != 0 {
		t.Fatalf("r3 mutated after failed group acquire: %+v", info3)
	}
	if info := s.GetResource("r2"); !info.Active || info.Holder != "bob" {
		t.Fatalf("r2 mutated: %+v", info)
	}
	// 单资源写入与续租仍可用，证明原租约没有被误删。
	if err := s.Write(WriteRequest{ResourceID: "r1", Holder: "alice", Token: l1.Token}); err != nil {
		t.Fatalf("write r1 after rollback: %v", err)
	}
	if _, err := s.Renew(RenewRequest{ResourceID: "r3", Holder: "alice", TTL: time.Minute}); err != nil {
		t.Fatalf("renew r3 after rollback: %v", err)
	}

	// 幂等重放失败结果（输入顺序不同但归一化后相同）。
	_, err2 := s.AcquireGroup(GroupRequest{
		ResourceIDs: []string{"r3", "r2", "r1"},
		Holder:      "alice",
		TTL:         time.Minute,
		RequestID:   "g-fail",
	})
	if !errors.As(err2, &unavailable) || unavailable.Resource != "r2" {
		t.Fatalf("replay failed request = %v, want same UnavailableError", err2)
	}
}

func TestGroupIdempotencyAndConflict(t *testing.T) {
	s, _ := newTestService()

	g, err := s.AcquireGroup(GroupRequest{
		ResourceIDs: []string{"b", "a"},
		Holder:      "alice",
		TTL:         time.Minute,
		RequestID:   "req-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	// 同号同内容（即使顺序不同）返回原结果。
	again, err := s.AcquireGroup(GroupRequest{
		ResourceIDs: []string{"a", "b"},
		Holder:      "alice",
		TTL:         time.Minute,
		RequestID:   "req-1",
	})
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if again.GroupID != g.GroupID || again.Tokens["a"] != g.Tokens["a"] || again.Tokens["b"] != g.Tokens["b"] {
		t.Fatalf("replay returned different result: %+v vs %+v", again, g)
	}

	cases := []GroupRequest{
		{ResourceIDs: []string{"a", "c"}, Holder: "alice", TTL: time.Minute, RequestID: "req-1"},
		{ResourceIDs: []string{"a", "b"}, Holder: "bob", TTL: time.Minute, RequestID: "req-1"},
		{ResourceIDs: []string{"a", "b"}, Holder: "alice", TTL: 2 * time.Minute, RequestID: "req-1"},
	}
	for i, req := range cases {
		if _, err := s.AcquireGroup(req); !errors.Is(err, ErrConflict) {
			t.Fatalf("conflict case %d: got %v, want ErrConflict", i, err)
		}
	}

	// 续租幂等：同号同内容返回原结果，期限变化冲突。
	r1, err := s.RenewGroup(GroupRenewRequest{GroupID: g.GroupID, Holder: "alice", TTL: 5 * time.Minute, RequestID: "renew-1"})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.RenewGroup(GroupRenewRequest{GroupID: g.GroupID, Holder: "alice", TTL: 5 * time.Minute, RequestID: "renew-1"})
	if err != nil {
		t.Fatalf("idempotent renew: %v", err)
	}
	if !r2.ExpiresAt.Equal(r1.ExpiresAt) {
		t.Fatalf("renew replay changed expiry: %v vs %v", r2.ExpiresAt, r1.ExpiresAt)
	}
	if _, err := s.RenewGroup(GroupRenewRequest{GroupID: g.GroupID, Holder: "alice", TTL: time.Second, RequestID: "renew-1"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("renew ttl conflict: %v", err)
	}

	// 释放幂等与持有者冲突。
	if err := s.ReleaseGroup(GroupOpRequest{GroupID: g.GroupID, Holder: "alice", RequestID: "rel-1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseGroup(GroupOpRequest{GroupID: g.GroupID, Holder: "alice", RequestID: "rel-1"}); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
	if err := s.ReleaseGroup(GroupOpRequest{GroupID: g.GroupID, Holder: "bob", RequestID: "rel-1"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("release holder conflict: %v", err)
	}
}

func TestGroupWriteRequiresMatchingVersionAndTokens(t *testing.T) {
	s, _ := newTestService()
	g, err := s.AcquireGroup(GroupRequest{
		ResourceIDs: []string{"r1", "r2"},
		Holder:      "alice",
		TTL:         time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	validTokens := map[string]int64{"r1": g.Tokens["r1"], "r2": g.Tokens["r2"]}
	if err := s.WriteGroup(GroupWriteRequest{GroupID: g.GroupID, Holder: "alice", Tokens: validTokens}); err != nil {
		t.Fatalf("valid group write: %v", err)
	}

	// 不能对组内成员走单资源写入、续租或释放。
	if err := s.Write(WriteRequest{ResourceID: "r1", Holder: "alice", Token: g.Tokens["r1"]}); !errors.Is(err, ErrSplitGroup) {
		t.Fatalf("single write on group member = %v, want ErrSplitGroup", err)
	}
	if _, err := s.Renew(RenewRequest{ResourceID: "r1", Holder: "alice", TTL: time.Minute}); !errors.Is(err, ErrSplitGroup) {
		t.Fatalf("single renew on group member = %v, want ErrSplitGroup", err)
	}
	if err := s.Release(ReleaseRequest{ResourceID: "r1", Holder: "alice"}); !errors.Is(err, ErrSplitGroup) {
		t.Fatalf("single release on group member = %v, want ErrSplitGroup", err)
	}

	// token 集合缺少成员或携带旧 token 都必须失败。
	if err := s.WriteGroup(GroupWriteRequest{
		GroupID: g.GroupID, Holder: "alice",
		Tokens: map[string]int64{"r1": g.Tokens["r1"]},
	}); !errors.Is(err, ErrInvalidSet) {
		t.Fatalf("partial token set = %v, want ErrInvalidSet", err)
	}
	if err := s.WriteGroup(GroupWriteRequest{
		GroupID: g.GroupID, Holder: "alice",
		Tokens: map[string]int64{"r1": g.Tokens["r1"] - 1, "r2": g.Tokens["r2"]},
	}); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("stale token = %v, want ErrInvalidToken", err)
	}
	if err := s.WriteGroup(GroupWriteRequest{
		GroupID: g.GroupID, Holder: "bob", Tokens: validTokens,
	}); !errors.Is(err, ErrNotHolder) {
		t.Fatalf("wrong holder = %v, want ErrNotHolder", err)
	}
	if err := s.WriteGroup(GroupWriteRequest{
		GroupID: g.GroupID + 100, Holder: "alice", Tokens: validTokens,
	}); !errors.Is(err, ErrInvalidGroup) {
		t.Fatalf("unknown version = %v, want ErrInvalidGroup", err)
	}
}

func TestGroupInvalidatedByTransferOfOneMember(t *testing.T) {
	s, _ := newTestService()
	g, err := s.AcquireGroup(GroupRequest{
		ResourceIDs: []string{"r1", "r2", "r3"},
		Holder:      "alice",
		TTL:         time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	oldTokens := map[string]int64{
		"r1": g.Tokens["r1"],
		"r2": g.Tokens["r2"],
		"r3": g.Tokens["r3"],
	}

	// r1 被 bob 接管：旧组合立即整体失效。
	t1, err := s.Transfer(TransferRequest{ResourceID: "r1", From: "alice", To: "bob", TTL: time.Minute})
	if err != nil {
		t.Fatalf("transfer member: %v", err)
	}
	if t1.Token <= g.Tokens["r1"] {
		t.Fatalf("transfer must issue new token")
	}

	// 旧组合不能写其它任何资源（即使 r2/r3 尚未过期）。
	if err := s.WriteGroup(GroupWriteRequest{GroupID: g.GroupID, Holder: "alice", Tokens: oldTokens}); !errors.Is(err, ErrInvalidGroup) {
		t.Fatalf("old group write after takeover = %v, want ErrInvalidGroup", err)
	}
	if _, err := s.RenewGroup(GroupRenewRequest{GroupID: g.GroupID, Holder: "alice", TTL: time.Minute}); !errors.Is(err, ErrInvalidGroup) {
		t.Fatalf("old group renew after takeover = %v, want ErrInvalidGroup", err)
	}

	// bob 只能单独写 r1；r2/r3 不再属于任何组合，恢复为 alice 的单资源租约。
	if err := s.Write(WriteRequest{ResourceID: "r1", Holder: "bob", Token: t1.Token}); err != nil {
		t.Fatalf("new owner write r1: %v", err)
	}
	for _, id := range []string{"r2", "r3"} {
		info := s.GetResource(id)
		if !info.Active || info.Holder != "alice" || info.GroupID != 0 {
			t.Fatalf("%s after takeover = %+v, want alice single lease", id, info)
		}
	}
	if err := s.Write(WriteRequest{ResourceID: "r2", Holder: "alice", Token: g.Tokens["r2"]}); err != nil {
		t.Fatalf("alice single write r2 after group break: %v", err)
	}
	info1 := s.GetResource("r1")
	if !info1.Active || info1.Holder != "bob" || info1.GroupID != 0 {
		t.Fatalf("r1 = %+v, want bob single lease", info1)
	}

	// 查询组合版本能看到失效原因。
	gi, err := s.GetGroup(g.GroupID)
	if err != nil {
		t.Fatal(err)
	}
	if gi.Active || gi.Reason != ReasonTakenOver {
		t.Fatalf("group info = %+v, want inactive takenover", gi)
	}
}

func TestGroupExpiryBreaksWholeGroup(t *testing.T) {
	s, clk := newTestService()
	g, err := s.AcquireGroup(GroupRequest{
		ResourceIDs: []string{"r1", "r2"},
		Holder:      "alice",
		TTL:         time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	clk.advance(time.Minute)
	reclaimed := s.ScanExpired()
	if fmt.Sprint(reclaimed) != fmt.Sprint([]string{"r1", "r2"}) {
		t.Fatalf("reclaimed = %v, want [r1 r2]", reclaimed)
	}

	tokens := map[string]int64{"r1": g.Tokens["r1"], "r2": g.Tokens["r2"]}
	if err := s.WriteGroup(GroupWriteRequest{GroupID: g.GroupID, Holder: "alice", Tokens: tokens}); !errors.Is(err, ErrInvalidGroup) {
		t.Fatalf("write expired group = %v, want ErrInvalidGroup", err)
	}
	for _, id := range []string{"r1", "r2"} {
		info := s.GetResource(id)
		if info.Active || info.Reason != ReasonExpired {
			t.Fatalf("%s = %+v, want inactive expired", id, info)
		}
	}

	// 过期后可被新持有者重新取得，token 继续递增。
	g2, err := s.AcquireGroup(GroupRequest{
		ResourceIDs: []string{"r1", "r2"},
		Holder:      "bob",
		TTL:         time.Minute,
	})
	if err != nil {
		t.Fatalf("reacquire after expiry: %v", err)
	}
	if g2.GroupID == g.GroupID {
		t.Fatalf("new group must get new version")
	}
	if g2.Tokens["r1"] <= g.Tokens["r1"] || g2.Tokens["r2"] <= g.Tokens["r2"] {
		t.Fatalf("tokens must increase after expiry")
	}
}

func TestRenewGroupExtendsAllMembersUniformly(t *testing.T) {
	s, clk := newTestService()
	g, err := s.AcquireGroup(GroupRequest{
		ResourceIDs: []string{"r1", "r2"},
		Holder:      "alice",
		TTL:         time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	clk.advance(30 * time.Second)

	rg, err := s.RenewGroup(GroupRenewRequest{GroupID: g.GroupID, Holder: "alice", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"r1", "r2"} {
		info := s.GetResource(id)
		if !info.ExpiresAt.Equal(rg.ExpiresAt) {
			t.Fatalf("%s expiry %v != group expiry %v", id, info.ExpiresAt, rg.ExpiresAt)
		}
	}
	clk.advance(30 * time.Second)
	// 首次租约本应在此时过期，但续租后仍然有效。
	if err := s.WriteGroup(GroupWriteRequest{
		GroupID: g.GroupID, Holder: "alice",
		Tokens: map[string]int64{"r1": g.Tokens["r1"], "r2": g.Tokens["r2"]},
	}); err != nil {
		t.Fatalf("write after renew: %v", err)
	}
}

func TestReleaseGroupIsAtomic(t *testing.T) {
	s, _ := newTestService()
	g, err := s.AcquireGroup(GroupRequest{
		ResourceIDs: []string{"r1", "r2"},
		Holder:      "alice",
		TTL:         time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseGroup(GroupOpRequest{GroupID: g.GroupID, Holder: "alice"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"r1", "r2"} {
		info := s.GetResource(id)
		if info.Active || info.Holder != "" || info.GroupID != 0 || info.Reason != ReasonReleased {
			t.Fatalf("%s = %+v, want released", id, info)
		}
	}
	if _, err := s.RenewGroup(GroupRenewRequest{GroupID: g.GroupID, Holder: "alice", TTL: time.Minute}); !errors.Is(err, ErrInvalidGroup) {
		t.Fatalf("renew released group = %v, want ErrInvalidGroup", err)
	}
}

func TestConcurrentOperationsNoDeadlock(t *testing.T) {
	s, _ := newTestService()

	// 预置一批单资源租约。
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("res%02d", i)
		if _, err := s.Acquire(AcquireRequest{ResourceID: id, Holder: "seed", TTL: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			holder := fmt.Sprintf("worker-%d", w)
			for i := 0; i < 200; i++ {
				ids := []string{
					fmt.Sprintf("res%02d", (w+i)%10),
					fmt.Sprintf("res%02d", (w+i+1)%10),
					fmt.Sprintf("fresh-%d-%d", w, i),
				}
				g, err := s.AcquireGroup(GroupRequest{
					ResourceIDs: ids,
					Holder:      holder,
					TTL:         200 * time.Millisecond,
				})
				if err != nil {
					// 被其他 worker 占住是合法结果；重试别的集合。
					if !errors.Is(err, ErrUnavailable) {
						t.Errorf("unexpected group error: %v", err)
						return
					}
					continue
				}
				tokens := map[string]int64{}
				for k, v := range g.Tokens {
					tokens[k] = v
				}
				if err := s.WriteGroup(GroupWriteRequest{GroupID: g.GroupID, Holder: holder, Tokens: tokens}); err != nil {
					t.Errorf("group write: %v", err)
					return
				}
				if _, err := s.RenewGroup(GroupRenewRequest{GroupID: g.GroupID, Holder: holder, TTL: time.Hour}); err != nil {
					t.Errorf("group renew: %v", err)
					return
				}
				_ = s.ReleaseGroup(GroupOpRequest{GroupID: g.GroupID, Holder: holder})
			}
		}(w)
	}

	// 并发起过期扫描与单资源交接。
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				s.ScanExpired()
				id := fmt.Sprintf("res%02d", (w+i)%10)
				_, _ = s.Transfer(TransferRequest{
					ResourceID: id,
					From:       "seed",
					To:         fmt.Sprintf("seed-%d", w),
					TTL:        time.Hour,
				})
			}
		}(w)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("concurrent operations deadlocked")
	}
}

func TestSingleOpsIdempotent(t *testing.T) {
	s, _ := newTestService()

	l1, err := s.Acquire(AcquireRequest{ResourceID: "r1", Holder: "alice", TTL: time.Minute, RequestID: "q1"})
	if err != nil {
		t.Fatal(err)
	}
	l2, err := s.Acquire(AcquireRequest{ResourceID: "r1", Holder: "alice", TTL: time.Minute, RequestID: "q1"})
	if err != nil {
		t.Fatalf("idempotent acquire: %v", err)
	}
	if l2.Token != l1.Token {
		t.Fatalf("acquire replay issued new token: %d vs %d", l1.Token, l2.Token)
	}
	if _, err := s.Acquire(AcquireRequest{ResourceID: "r1", Holder: "bob", TTL: time.Minute, RequestID: "q1"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("acquire holder conflict: %v", err)
	}

	n1, err := s.Renew(RenewRequest{ResourceID: "r1", Holder: "alice", TTL: 2 * time.Minute, RequestID: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	n2, err := s.Renew(RenewRequest{ResourceID: "r1", Holder: "alice", TTL: 2 * time.Minute, RequestID: "n1"})
	if err != nil {
		t.Fatalf("idempotent renew: %v", err)
	}
	if !n2.ExpiresAt.Equal(n1.ExpiresAt) {
		t.Fatalf("renew replay changed expiry")
	}

	if err := s.Release(ReleaseRequest{ResourceID: "r1", Holder: "alice", RequestID: "d1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ReleaseRequest{ResourceID: "r1", Holder: "alice", RequestID: "d1"}); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}

	// 交接幂等：重放应得到同一个新 token，而不是再次递增。
	_, _ = s.Acquire(AcquireRequest{ResourceID: "r2", Holder: "alice", TTL: time.Minute})
	x1, err := s.Transfer(TransferRequest{ResourceID: "r2", From: "alice", To: "carol", TTL: time.Minute, RequestID: "x1"})
	if err != nil {
		t.Fatal(err)
	}
	x2, err := s.Transfer(TransferRequest{ResourceID: "r2", From: "alice", To: "carol", TTL: time.Minute, RequestID: "x1"})
	if err != nil {
		t.Fatalf("idempotent transfer: %v", err)
	}
	if x2.Token != x1.Token {
		t.Fatalf("transfer replay issued new token: %d vs %d", x1.Token, x2.Token)
	}
}

func TestQueryReasons(t *testing.T) {
	s, clk := newTestService()

	if info := s.GetResource("ghost"); info.Active || info.Token != 0 || info.Reason != ReasonNeverAcquired {
		t.Fatalf("never acquired = %+v", info)
	}

	l, err := s.Acquire(AcquireRequest{ResourceID: "single", Holder: "alice", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if info := s.GetResource("single"); !info.Active || info.Token != l.Token || info.Holder != "alice" {
		t.Fatalf("active single = %+v", info)
	}

	g, err := s.AcquireGroup(GroupRequest{ResourceIDs: []string{"g1", "g2"}, Holder: "alice", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if info := s.GetResource("g1"); !info.Active || info.GroupID != g.GroupID {
		t.Fatalf("active group member = %+v", info)
	}
	gi, err := s.GetGroup(g.GroupID)
	if err != nil || !gi.Active || gi.Tokens["g2"] != g.Tokens["g2"] {
		t.Fatalf("active group info = %+v, err %v", gi, err)
	}

	if err := s.Release(ReleaseRequest{ResourceID: "single", Holder: "alice"}); err != nil {
		t.Fatal(err)
	}
	if info := s.GetResource("single"); info.Active || info.Reason != ReasonReleased || info.Token != l.Token {
		t.Fatalf("released single = %+v", info)
	}

	clk.advance(time.Minute)
	info := s.GetResource("g1")
	if info.Active || info.Reason != ReasonExpired {
		t.Fatalf("expired group member = %+v", info)
	}
	if _, err := s.GetGroup(g.GroupID); err != nil {
		t.Fatalf("expired group should remain queryable: %v", err)
	}
	gi, _ = s.GetGroup(g.GroupID)
	if gi.Active || gi.Reason != ReasonExpired {
		t.Fatalf("expired group info = %+v", gi)
	}
	if _, err := s.GetGroup(99999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown group = %v, want ErrNotFound", err)
	}

	// ListResources 按编号排序。
	list := s.ListResources()
	if len(list) != 3 || list[0].ResourceID != "g1" {
		t.Fatalf("list = %+v", list)
	}
}
