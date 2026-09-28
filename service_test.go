package fencinglock

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fakeClock 是可手动推进的时钟，用于确定性测试租约到期。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestService(t *testing.T, clock *fakeClock) *Service {
	t.Helper()
	svc, err := Open(filepath.Join(t.TempDir(), "lease.db"), WithClock(clock.Now))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { svc.Close() })
	return svc
}

func mustAcquire(t *testing.T, svc *Service, resource, holder string, ttl time.Duration, reqID string) Lease {
	t.Helper()
	l, err := svc.Acquire(context.Background(), resource, holder, ttl, reqID)
	if err != nil {
		t.Fatalf("Acquire(%s, %s): %v", resource, holder, err)
	}
	return l
}

func TestAcquireAndStatus(t *testing.T) {
	clock := newFakeClock()
	svc := newTestService(t, clock)
	ctx := context.Background()

	l := mustAcquire(t, svc, "res1", "alice", time.Minute, "req-1")
	if l.Token != 1 {
		t.Fatalf("first token = %d, want 1", l.Token)
	}
	if want := clock.Now().Add(time.Minute); !l.ExpiresAt.Equal(want) {
		t.Fatalf("ExpiresAt = %v, want %v", l.ExpiresAt, want)
	}

	st, err := svc.Status(ctx, "res1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if !st.Held || st.Holder != "alice" || st.Token != 1 {
		t.Fatalf("unexpected status: %+v", st)
	}

	st, err = svc.Status(ctx, "unknown")
	if err != nil {
		t.Fatalf("Status(unknown): %v", err)
	}
	if st.Held || st.DataVersion != 0 {
		t.Fatalf("unexpected status for unknown resource: %+v", st)
	}
}

func TestTokensStrictlyIncreasingAndNeverReused(t *testing.T) {
	clock := newFakeClock()
	svc := newTestService(t, clock)

	l1 := mustAcquire(t, svc, "a", "alice", time.Minute, "req-1")
	l2 := mustAcquire(t, svc, "b", "bob", time.Minute, "req-2")
	if !(l1.Token < l2.Token) {
		t.Fatalf("tokens not increasing: %d, %d", l1.Token, l2.Token)
	}

	// 释放后重新获取，token 不得复用。
	if err := svc.Release(context.Background(), "a", "alice", l1.Token, "req-3"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	l3 := mustAcquire(t, svc, "a", "carol", time.Minute, "req-4")
	if l3.Token <= l2.Token {
		t.Fatalf("reused token: got %d after %d", l3.Token, l2.Token)
	}

	// 过期后重新获取，token 继续递增。
	clock.Advance(2 * time.Minute)
	l4 := mustAcquire(t, svc, "b", "dave", time.Minute, "req-5")
	if l4.Token <= l3.Token {
		t.Fatalf("token not increasing after expiry: got %d after %d", l4.Token, l3.Token)
	}
}

func TestTokenSurvivesRestart(t *testing.T) {
	clock := newFakeClock()
	path := filepath.Join(t.TempDir(), "lease.db")
	ctx := context.Background()

	svc, err := Open(path, WithClock(clock.Now))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	l1 := mustAcquire(t, svc, "res", "alice", time.Minute, "req-1")
	if _, err := svc.WriteProtected(ctx, "res", l1.Token, []byte("v1"), "req-2"); err != nil {
		t.Fatalf("WriteProtected: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 重启后：token 继续单调、业务数据仍在。
	svc2, err := Open(path, WithClock(clock.Now))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer svc2.Close()
	clock.Advance(2 * time.Minute) // 让旧租约过期
	l2 := mustAcquire(t, svc2, "res", "bob", time.Minute, "req-3")
	if l2.Token <= l1.Token {
		t.Fatalf("token reused across restart: %d <= %d", l2.Token, l1.Token)
	}
	data, version, token, err := svc2.ReadData(ctx, "res")
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if string(data) != "v1" || version != 1 || token != l1.Token {
		t.Fatalf("data lost across restart: %q v%d t%d", data, version, token)
	}
}

func TestAcquireBusyUntilExpiry(t *testing.T) {
	clock := newFakeClock()
	svc := newTestService(t, clock)

	mustAcquire(t, svc, "res", "alice", time.Minute, "req-1")
	_, err := svc.Acquire(context.Background(), "res", "bob", time.Minute, "req-2")
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("Acquire by bob = %v, want ErrBusy", err)
	}

	clock.Advance(time.Minute + time.Nanosecond)
	if _, err := svc.Acquire(context.Background(), "res", "bob", time.Minute, "req-3"); err != nil {
		t.Fatalf("Acquire after expiry: %v", err)
	}
}

func TestRenew(t *testing.T) {
	clock := newFakeClock()
	svc := newTestService(t, clock)
	ctx := context.Background()

	l := mustAcquire(t, svc, "res", "alice", time.Minute, "req-1")
	clock.Advance(30 * time.Second)

	renewed, err := svc.Renew(ctx, "res", "alice", l.Token, time.Minute, "req-2")
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if renewed.Token != l.Token {
		t.Fatalf("renew changed token: %d -> %d", l.Token, renewed.Token)
	}
	if want := clock.Now().Add(time.Minute); !renewed.ExpiresAt.Equal(want) {
		t.Fatalf("ExpiresAt = %v, want %v", renewed.ExpiresAt, want)
	}

	// token 不匹配。
	if _, err := svc.Renew(ctx, "res", "alice", l.Token+99, time.Minute, "req-3"); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("Renew wrong token = %v, want ErrStaleToken", err)
	}
	// 非持有者。
	if _, err := svc.Renew(ctx, "res", "mallory", l.Token, time.Minute, "req-4"); !errors.Is(err, ErrNotHolder) {
		t.Fatalf("Renew wrong holder = %v, want ErrNotHolder", err)
	}
	// 过期后旧持有者迟到续约。
	clock.Advance(2 * time.Minute)
	if _, err := svc.Renew(ctx, "res", "alice", l.Token, time.Minute, "req-5"); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("Renew after expiry = %v, want ErrLeaseExpired", err)
	}
}

func TestRelease(t *testing.T) {
	clock := newFakeClock()
	svc := newTestService(t, clock)
	ctx := context.Background()

	l := mustAcquire(t, svc, "res", "alice", time.Minute, "req-1")
	if err := svc.Release(ctx, "res", "alice", l.Token, "req-2"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	st, err := svc.Status(ctx, "res")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Held {
		t.Fatalf("lease still held after release: %+v", st)
	}

	// 释放后他人可立即获取。
	l2 := mustAcquire(t, svc, "res", "bob", time.Minute, "req-3")
	// 旧持有者迟到的释放不得影响新租约。
	if err := svc.Release(ctx, "res", "alice", l.Token, "req-4"); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("stale release = %v, want ErrStaleToken", err)
	}
	st, _ = svc.Status(ctx, "res")
	if !st.Held || st.Holder != "bob" {
		t.Fatalf("bob's lease disturbed by stale release: %+v", st)
	}
	// 过期后旧持有者不能释放。
	clock.Advance(2 * time.Minute)
	if err := svc.Release(ctx, "res", "bob", l2.Token, "req-5"); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("release after expiry = %v, want ErrLeaseExpired", err)
	}
}

func TestIdempotency(t *testing.T) {
	clock := newFakeClock()
	svc := newTestService(t, clock)
	ctx := context.Background()

	// Acquire 重放：同一请求号返回同一 token，不分配新 token。
	l1 := mustAcquire(t, svc, "res", "alice", time.Minute, "req-acq")
	l1b, err := svc.Acquire(ctx, "res", "alice", time.Minute, "req-acq")
	if err != nil {
		t.Fatalf("replay Acquire: %v", err)
	}
	if l1b.Token != l1.Token {
		t.Fatalf("replayed acquire allocated new token: %d != %d", l1b.Token, l1.Token)
	}
	// 同一请求号换内容 -> 幂等冲突。
	if _, err := svc.Acquire(ctx, "res", "alice", 2*time.Minute, "req-acq"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("acquire conflict = %v, want ErrIdempotencyConflict", err)
	}

	// Renew 重放。
	r1, err := svc.Renew(ctx, "res", "alice", l1.Token, time.Minute, "req-renew")
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	r2, err := svc.Renew(ctx, "res", "alice", l1.Token, time.Minute, "req-renew")
	if err != nil {
		t.Fatalf("replay Renew: %v", err)
	}
	if !r1.ExpiresAt.Equal(r2.ExpiresAt) {
		t.Fatalf("replayed renew differs: %v != %v", r1.ExpiresAt, r2.ExpiresAt)
	}
	if _, err := svc.Renew(ctx, "res", "alice", l1.Token, 5*time.Minute, "req-renew"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("renew conflict = %v, want ErrIdempotencyConflict", err)
	}

	// Write 重放：版本号不重复递增。
	v1, err := svc.WriteProtected(ctx, "res", l1.Token, []byte("data"), "req-write")
	if err != nil {
		t.Fatalf("WriteProtected: %v", err)
	}
	v2, err := svc.WriteProtected(ctx, "res", l1.Token, []byte("data"), "req-write")
	if err != nil {
		t.Fatalf("replay WriteProtected: %v", err)
	}
	if v1 != v2 {
		t.Fatalf("replayed write bumped version: %d != %d", v1, v2)
	}
	if _, err := svc.WriteProtected(ctx, "res", l1.Token, []byte("other"), "req-write"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("write conflict = %v, want ErrIdempotencyConflict", err)
	}

	// Release 重放：第二次同请求号释放应成功（幂等），而不是报错。
	if err := svc.Release(ctx, "res", "alice", l1.Token, "req-rel"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := svc.Release(ctx, "res", "alice", l1.Token, "req-rel"); err != nil {
		t.Fatalf("replay Release: %v", err)
	}
	if err := svc.Release(ctx, "res", "alice", l1.Token+1, "req-rel"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("release conflict = %v, want ErrIdempotencyConflict", err)
	}
}

func TestWriteProtectedFencing(t *testing.T) {
	clock := newFakeClock()
	svc := newTestService(t, clock)
	ctx := context.Background()

	l1 := mustAcquire(t, svc, "res", "alice", time.Minute, "req-1")
	v, err := svc.WriteProtected(ctx, "res", l1.Token, []byte("from-alice"), "req-2")
	if err != nil {
		t.Fatalf("WriteProtected: %v", err)
	}
	if v != 1 {
		t.Fatalf("version = %d, want 1", v)
	}

	// 租约过期、bob 重新获取后，alice 的旧 token 写入必须失败且数据不变。
	clock.Advance(2 * time.Minute)
	l2 := mustAcquire(t, svc, "res", "bob", time.Minute, "req-3")
	if _, err := svc.WriteProtected(ctx, "res", l1.Token, []byte("stale-write"), "req-4"); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("stale write = %v, want ErrStaleToken", err)
	}
	data, version, token, err := svc.ReadData(ctx, "res")
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if string(data) != "from-alice" || version != 1 || token != l1.Token {
		t.Fatalf("failed write mutated data: %q v%d t%d", data, version, token)
	}

	// 新持有者写入成功，版本递增。
	if v, err := svc.WriteProtected(ctx, "res", l2.Token, []byte("from-bob"), "req-5"); err != nil || v != 2 {
		t.Fatalf("bob write = v%d, %v; want v2", v, err)
	}

	// 租约过期后任何人都不能写。
	clock.Advance(2 * time.Minute)
	if _, err := svc.WriteProtected(ctx, "res", l2.Token, []byte("late"), "req-6"); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("write after expiry = %v, want ErrLeaseExpired", err)
	}
	data, version, _, _ = svc.ReadData(ctx, "res")
	if string(data) != "from-bob" || version != 2 {
		t.Fatalf("expired write mutated data: %q v%d", data, version)
	}
}

func TestInvalidArguments(t *testing.T) {
	clock := newFakeClock()
	svc := newTestService(t, clock)
	ctx := context.Background()

	cases := []struct {
		name string
		fn   func() error
	}{
		{"acquire empty resource", func() error { _, e := svc.Acquire(ctx, "", "a", time.Minute, "r1"); return e }},
		{"acquire empty holder", func() error { _, e := svc.Acquire(ctx, "r", "", time.Minute, "r2"); return e }},
		{"acquire zero ttl", func() error { _, e := svc.Acquire(ctx, "r", "a", 0, "r3"); return e }},
		{"acquire ttl too large", func() error { _, e := svc.Acquire(ctx, "r", "a", MaxTTL+time.Second, "r4"); return e }},
		{"acquire empty request id", func() error { _, e := svc.Acquire(ctx, "r", "a", time.Minute, ""); return e }},
		{"renew bad token", func() error { _, e := svc.Renew(ctx, "r", "a", 0, time.Minute, "r5"); return e }},
		{"release empty holder", func() error { return svc.Release(ctx, "r", "", 1, "r6") }},
		{"write bad token", func() error { _, e := svc.WriteProtected(ctx, "r", -1, []byte("x"), "r7"); return e }},
		{"write empty request id", func() error { _, e := svc.WriteProtected(ctx, "r", 1, []byte("x"), ""); return e }},
		{"status empty resource", func() error { _, e := svc.Status(ctx, ""); return e }},
	}
	for _, c := range cases {
		if err := c.fn(); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s = %v, want ErrInvalidArgument", c.name, err)
		}
	}
}

func TestErrorContext(t *testing.T) {
	clock := newFakeClock()
	svc := newTestService(t, clock)
	ctx := context.Background()

	mustAcquire(t, svc, "res", "alice", time.Minute, "req-1")
	err := func() error { _, e := svc.Acquire(ctx, "res", "bob", time.Minute, "req-2"); return e }()
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("want ErrBusy, got %v", err)
	}
	// 错误信息应携带资源与持有者上下文。
	if got := fmt.Sprint(err); got == ErrBusy.Error() {
		t.Fatalf("error lacks context: %q", got)
	}
}
