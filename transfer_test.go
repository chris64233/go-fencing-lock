package fencinglock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func mustTransfer(t *testing.T, svc *Service, resource, holder string, token int64, target string, ttl time.Duration, reqID string) Lease {
	t.Helper()
	l, err := svc.Transfer(context.Background(), resource, holder, token, target, ttl, reqID)
	if err != nil {
		t.Fatalf("Transfer(%s, %s -> %s): %v", resource, holder, target, err)
	}
	return l
}

// TestTransferHandsOffOwnership 交接确认后生成递增的 fencing token，
// 旧持有者即使本地租约尚未过期也只能得到版本错误。
func TestTransferHandsOffOwnership(t *testing.T) {
	clock := newFakeClock()
	svc := newTestService(t, clock)
	ctx := context.Background()

	alice := mustAcquire(t, svc, "res", "alice", time.Hour, "acq-1")
	bob := mustTransfer(t, svc, "res", "alice", alice.Token, "bob", 30*time.Minute, "xfer-1")
	if bob.Token <= alice.Token {
		t.Fatalf("transfer token = %d, want > %d", bob.Token, alice.Token)
	}
	if bob.Holder != "bob" {
		t.Fatalf("new holder = %q, want bob", bob.Holder)
	}
	wantExpiry := clock.Now().Add(30 * time.Minute)
	if !bob.ExpiresAt.Equal(wantExpiry) {
		t.Fatalf("handover expiry = %s, want %s", bob.ExpiresAt, wantExpiry)
	}

	// 时钟未推进：alice 的本地租约远未到期，但旧 token 已失效。
	var stale *StaleTokenError
	_, err := svc.WriteProtected(ctx, "res", alice.Token, []byte("late"), "w-late")
	if !errors.As(err, &stale) {
		t.Fatalf("late write error = %v, want StaleTokenError", err)
	}
	if !errors.Is(err, ErrStaleToken) {
		t.Fatalf("late write error = %v, want ErrStaleToken", err)
	}
	if stale.Provided != alice.Token || stale.Current != bob.Token {
		t.Fatalf("stale error versions = (%d, %d), want (%d, %d)",
			stale.Provided, stale.Current, alice.Token, bob.Token)
	}

	// 新持有者可以正常写入。
	if _, err := svc.WriteProtected(ctx, "res", bob.Token, []byte("v1"), "w-1"); err != nil {
		t.Fatalf("bob write: %v", err)
	}
}

// TestTransferIdempotent 相同转移请求返回原结果；同一请求号换锁名
// 或版本返回幂等冲突。
func TestTransferIdempotent(t *testing.T) {
	clock := newFakeClock()
	svc := newTestService(t, clock)
	ctx := context.Background()

	alice := mustAcquire(t, svc, "res", "alice", time.Hour, "acq-1")
	first := mustTransfer(t, svc, "res", "alice", alice.Token, "bob", time.Hour, "xfer-1")

	again := mustTransfer(t, svc, "res", "alice", alice.Token, "bob", time.Hour, "xfer-1")
	if again != first {
		t.Fatalf("replayed transfer = %+v, want %+v", again, first)
	}

	// 同一请求号但锁名或版本不同：冲突。
	if _, err := svc.Transfer(ctx, "other", "alice", alice.Token, "bob", time.Hour, "xfer-1"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("transfer with different resource = %v, want ErrIdempotencyConflict", err)
	}
	if _, err := svc.Transfer(ctx, "res", "alice", alice.Token+1, "bob", time.Hour, "xfer-1"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("transfer with different token = %v, want ErrIdempotencyConflict", err)
	}

	// 重放不得产生新的 token 或重复的时间线记录。
	history, err := svc.History(ctx, "res")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("history entries = %d, want 2 (no duplicate transfer)", len(history))
	}
	st, err := svc.Status(ctx, "res")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Token != first.Token {
		t.Fatalf("current token = %d, want %d (replay must not allocate)", st.Token, first.Token)
	}
}

// TestTransferFailureKeepsOriginalLease 转移失败时原租约按原期限
// 继续有效。
func TestTransferFailureKeepsOriginalLease(t *testing.T) {
	clock := newFakeClock()
	svc := newTestService(t, clock)
	ctx := context.Background()

	alice := mustAcquire(t, svc, "res", "alice", time.Hour, "acq-1")

	// 版本错误：转移被拒绝。
	if _, err := svc.Transfer(ctx, "res", "alice", alice.Token+99, "bob", time.Hour, "xfer-bad"); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("transfer with wrong token = %v, want ErrStaleToken", err)
	}
	// 非持有者：转移被拒绝。
	if _, err := svc.Transfer(ctx, "res", "mallory", alice.Token, "bob", time.Hour, "xfer-bad-2"); !errors.Is(err, ErrNotHolder) {
		t.Fatalf("transfer by non-holder = %v, want ErrNotHolder", err)
	}

	// 原租约未受影响：持有者、token、到期时间保持原样。
	st, err := svc.Status(ctx, "res")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if !st.Held || st.Holder != "alice" || st.Token != alice.Token || !st.ExpiresAt.Equal(alice.ExpiresAt) {
		t.Fatalf("lease changed after failed transfer: %+v, want alice/%d until %s",
			st, alice.Token, alice.ExpiresAt)
	}
	// alice 仍可正常写入与续约。
	if _, err := svc.WriteProtected(ctx, "res", alice.Token, []byte("ok"), "w-1"); err != nil {
		t.Fatalf("alice write after failed transfer: %v", err)
	}
	if _, err := svc.Renew(ctx, "res", "alice", alice.Token, time.Hour, "ren-1"); err != nil {
		t.Fatalf("alice renew after failed transfer: %v", err)
	}

	// 租约过期后转移同样失败，且迟到转移不会复活租约。
	clock.Advance(2 * time.Hour)
	if _, err := svc.Transfer(ctx, "res", "alice", alice.Token, "bob", time.Hour, "xfer-late"); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("transfer after expiry = %v, want ErrLeaseExpired", err)
	}
	st, err = svc.Status(ctx, "res")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Held {
		t.Fatalf("lease resurrected by failed transfer: %+v", st)
	}
}

// TestTransferHistoryTimeline 时间线展示每次持有者、版本与失效原因，
// 被拒绝的操作不会留下记录。
func TestTransferHistoryTimeline(t *testing.T) {
	clock := newFakeClock()
	svc := newTestService(t, clock)
	ctx := context.Background()

	alice := mustAcquire(t, svc, "res", "alice", time.Hour, "acq-1")
	bob := mustTransfer(t, svc, "res", "alice", alice.Token, "bob", time.Hour, "xfer-1")

	// 旧版本的迟到写入被拒绝，不得记成又一次交接。
	if _, err := svc.WriteProtected(ctx, "res", alice.Token, []byte("late"), "w-late"); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("late write = %v, want ErrStaleToken", err)
	}
	if _, err := svc.Transfer(ctx, "res", "alice", alice.Token, "carol", time.Hour, "xfer-late"); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("late transfer = %v, want ErrStaleToken", err)
	}

	if err := svc.Release(ctx, "res", "bob", bob.Token, "rel-1"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	carol := mustAcquire(t, svc, "res", "carol", time.Hour, "acq-2")
	clock.Advance(2 * time.Hour)
	dave := mustAcquire(t, svc, "res", "dave", time.Hour, "acq-3")

	history, err := svc.History(ctx, "res")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 4 {
		t.Fatalf("history entries = %d, want 4: %+v", len(history), history)
	}
	type want struct {
		holder string
		token  int64
		reason string
	}
	wants := []want{
		{"alice", alice.Token, ReasonTransferred},
		{"bob", bob.Token, ReasonReleased},
		{"carol", carol.Token, ReasonExpired},
		{"dave", dave.Token, ""},
	}
	for i, w := range wants {
		e := history[i]
		if e.Holder != w.holder || e.Token != w.token || e.Reason != w.reason {
			t.Errorf("history[%d] = (%s, %d, %q), want (%s, %d, %q)",
				i, e.Holder, e.Token, e.Reason, w.holder, w.token, w.reason)
		}
		if e.AcquiredAt.IsZero() {
			t.Errorf("history[%d] missing AcquiredAt", i)
		}
		if w.reason == "" && !e.InvalidatedAt.IsZero() {
			t.Errorf("history[%d] active entry has InvalidatedAt %s", i, e.InvalidatedAt)
		}
	}
	// 过期失效的时刻是租约到期时刻，而非被顶替的时刻。
	if !history[2].InvalidatedAt.Equal(carol.ExpiresAt) {
		t.Errorf("expired entry invalidated at %s, want expiry %s",
			history[2].InvalidatedAt, carol.ExpiresAt)
	}
	// 交接先后可由时间线判断：alice 的失效时刻不晚于 bob 的就任时刻。
	if history[0].InvalidatedAt.After(history[1].AcquiredAt) {
		t.Errorf("transfer order inconsistent: %+v vs %+v", history[0], history[1])
	}
}

// TestStatusReadableAfterFailedWrite 资源写入失败后仍能读取当前锁
// 状态与时间线。
func TestStatusReadableAfterFailedWrite(t *testing.T) {
	clock := newFakeClock()
	svc := newTestService(t, clock)
	ctx := context.Background()

	alice := mustAcquire(t, svc, "res", "alice", time.Hour, "acq-1")
	bob := mustTransfer(t, svc, "res", "alice", alice.Token, "bob", time.Hour, "xfer-1")
	if _, err := svc.WriteProtected(ctx, "res", alice.Token, []byte("late"), "w-late"); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("late write = %v, want ErrStaleToken", err)
	}

	st, err := svc.Status(ctx, "res")
	if err != nil {
		t.Fatalf("Status after failed write: %v", err)
	}
	if !st.Held || st.Holder != "bob" || st.Token != bob.Token {
		t.Fatalf("status after failed write = %+v, want bob/%d held", st, bob.Token)
	}
	history, err := svc.History(ctx, "res")
	if err != nil {
		t.Fatalf("History after failed write: %v", err)
	}
	if len(history) != 2 || history[0].Reason != ReasonTransferred {
		t.Fatalf("history after failed write = %+v, want single transfer", history)
	}
}

// TestConcurrentTransferVsWrites 旧持有者写入与转移确认并发到达：
// 只有当前版本可以修改资源，数据版本连续无空洞。
func TestConcurrentTransferVsWrites(t *testing.T) {
	svc, err := Open(t.TempDir() + "/lease.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { svc.Close() })
	ctx := context.Background()

	alice, err := svc.Acquire(ctx, "hot", "alice", time.Minute, "acq-1")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	const writers = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	// 旧持有者并发写入：只有在转移确认前到达的才可能成功。
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			for i := 0; ; i++ {
				_, err := svc.WriteProtected(ctx, "hot", alice.Token,
					[]byte(fmt.Sprintf("w%d-%d", id, i)), fmt.Sprintf("w-%d-%d", id, i))
				if err != nil {
					if !errors.Is(err, ErrStaleToken) && !errors.Is(err, ErrLeaseExpired) {
						t.Errorf("old holder write error = %v, want stale/expired", err)
					}
					return
				}
			}
		}(w)
	}
	// 并发转移确认。
	var bob Lease
	var transferErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		bob, transferErr = svc.Transfer(ctx, "hot", "alice", alice.Token, "bob", time.Minute, "xfer-1")
	}()
	close(start)
	wg.Wait()

	if transferErr != nil {
		t.Fatalf("Transfer: %v", transferErr)
	}
	if bob.Token <= alice.Token {
		t.Fatalf("transfer token = %d, want > %d", bob.Token, alice.Token)
	}

	// 转移确认后旧版本写入一律被拒绝，且错误同时携带两个版本。
	var stale *StaleTokenError
	_, err = svc.WriteProtected(ctx, "hot", alice.Token, []byte("late"), "w-late")
	if !errors.As(err, &stale) || stale.Provided != alice.Token || stale.Current != bob.Token {
		t.Fatalf("late write after transfer = %v, want StaleTokenError(%d -> %d)",
			err, alice.Token, bob.Token)
	}

	// 只有当前版本可以修改资源：新持有者写入成功，版本连续。
	st, err := svc.Status(ctx, "hot")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	before := st.DataVersion
	v, err := svc.WriteProtected(ctx, "hot", bob.Token, []byte("bob"), "w-bob")
	if err != nil {
		t.Fatalf("bob write: %v", err)
	}
	if v != before+1 {
		t.Fatalf("data version = %d, want %d (no gaps)", v, before+1)
	}

	// 时间线恰好记录一次交接，并发拒绝不产生额外记录。
	history, err := svc.History(ctx, "hot")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 2 || history[0].Holder != "alice" || history[0].Reason != ReasonTransferred ||
		history[1].Holder != "bob" || history[1].Token != bob.Token {
		t.Fatalf("history = %+v, want single alice->bob transfer", history)
	}
}

// TestConcurrentExpiryVsWrites 旧持有者写入与租约过期并发到达：
// 过期后旧版本一律被拒，新持有者凭更高 token 接管资源。
func TestConcurrentExpiryVsWrites(t *testing.T) {
	svc, err := Open(t.TempDir() + "/lease.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { svc.Close() })
	ctx := context.Background()

	alice, err := svc.Acquire(ctx, "hot", "alice", 30*time.Millisecond, "acq-1")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	const writers = 8
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; ; i++ {
				_, err := svc.WriteProtected(ctx, "hot", alice.Token,
					[]byte(fmt.Sprintf("w%d-%d", id, i)), fmt.Sprintf("w-%d-%d", id, i))
				if err != nil {
					if !errors.Is(err, ErrLeaseExpired) && !errors.Is(err, ErrStaleToken) {
						t.Errorf("write after expiry = %v, want expired/stale", err)
					}
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// 租约已过期：旧持有者迟到写入只能得到过期/版本错误。
	if _, err := svc.WriteProtected(ctx, "hot", alice.Token, []byte("late"), "w-late"); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late write after expiry = %v, want ErrLeaseExpired", err)
	}

	// 新持有者接管并获得更高 token，只有它能继续修改资源。
	carol, err := svc.Acquire(ctx, "hot", "carol", time.Minute, "acq-2")
	if err != nil {
		t.Fatalf("carol Acquire: %v", err)
	}
	if carol.Token <= alice.Token {
		t.Fatalf("carol token = %d, want > %d", carol.Token, alice.Token)
	}
	var stale *StaleTokenError
	if _, err := svc.WriteProtected(ctx, "hot", alice.Token, []byte("late2"), "w-late2"); !errors.As(err, &stale) ||
		stale.Provided != alice.Token || stale.Current != carol.Token {
		t.Fatalf("old token write after takeover = %v, want StaleTokenError(%d -> %d)",
			err, alice.Token, carol.Token)
	}
	st, err := svc.Status(ctx, "hot")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	v, err := svc.WriteProtected(ctx, "hot", carol.Token, []byte("carol"), "w-carol")
	if err != nil {
		t.Fatalf("carol write: %v", err)
	}
	if v != st.DataVersion+1 {
		t.Fatalf("data version = %d, want %d (no gaps)", v, st.DataVersion+1)
	}
}
