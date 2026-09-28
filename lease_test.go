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

// fakeClock 是可手动推进的时间源，用于确定性地测试租约到期。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

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

func openTest(t *testing.T, clock *fakeClock) (*Service, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "leases.db")
	var opts []Option
	if clock != nil {
		opts = append(opts, WithClock(clock.now))
	}
	svc, err := Open(path, opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { svc.Close() })
	return svc, path
}

func TestAcquireGrantsMonotonicTokens(t *testing.T) {
	svc, _ := openTest(t, nil)
	ctx := context.Background()

	l1, err := svc.Acquire(ctx, "res", "alice", time.Minute)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if l1.Token != 1 {
		t.Fatalf("first token = %d, want 1", l1.Token)
	}

	if _, err := svc.Acquire(ctx, "res", "bob", time.Minute); !errors.Is(err, ErrBusy) {
		t.Fatalf("acquire while held: err = %v, want ErrBusy", err)
	}

	if err := svc.Release(ctx, "res", "alice", l1.Token, "rel-1"); err != nil {
		t.Fatalf("release: %v", err)
	}
	l2, err := svc.Acquire(ctx, "res", "bob", time.Minute)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if l2.Token <= l1.Token {
		t.Fatalf("token not monotonic: %d then %d", l1.Token, l2.Token)
	}

	// 不同资源的 token 计数相互独立。
	other, err := svc.Acquire(ctx, "other", "alice", time.Minute)
	if err != nil {
		t.Fatalf("acquire other resource: %v", err)
	}
	if other.Token != 1 {
		t.Fatalf("other resource first token = %d, want 1", other.Token)
	}
}

func TestAcquireAfterExpiryIssuesNewToken(t *testing.T) {
	clock := newFakeClock()
	svc, _ := openTest(t, clock)
	ctx := context.Background()

	l1, err := svc.Acquire(ctx, "res", "alice", 10*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	clock.advance(11 * time.Second)

	l2, err := svc.Acquire(ctx, "res", "bob", 10*time.Second)
	if err != nil {
		t.Fatalf("acquire after expiry: %v", err)
	}
	if l2.Token != l1.Token+1 {
		t.Fatalf("tokens = %d, %d; want strictly increasing", l1.Token, l2.Token)
	}
}

func TestRenewRequiresHolderAndLiveLease(t *testing.T) {
	clock := newFakeClock()
	svc, _ := openTest(t, clock)
	ctx := context.Background()

	l, err := svc.Acquire(ctx, "res", "alice", 10*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// 陈旧 token。
	if _, err := svc.Renew(ctx, "res", "alice", l.Token+99, "r1", 10*time.Second); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("renew with wrong token: err = %v, want ErrStaleToken", err)
	}
	// 错误的持有者。
	if _, err := svc.Renew(ctx, "res", "mallory", l.Token, "r2", 10*time.Second); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("renew by non-holder: err = %v, want ErrStaleToken", err)
	}

	// 正常续约，到期时间被延长。
	clock.advance(8 * time.Second)
	renewed, err := svc.Renew(ctx, "res", "alice", l.Token, "r3", 10*time.Second)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if !renewed.ExpiresAt.After(l.ExpiresAt) {
		t.Fatalf("renew did not extend expiry: %v -> %v", l.ExpiresAt, renewed.ExpiresAt)
	}

	// 到期后旧持有者迟到续约，必须失败。
	clock.advance(11 * time.Second)
	if _, err := svc.Renew(ctx, "res", "alice", l.Token, "r4", 10*time.Second); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late renew: err = %v, want ErrLeaseExpired", err)
	}
}

func TestRenewIdempotency(t *testing.T) {
	svc, _ := openTest(t, nil)
	ctx := context.Background()

	l, err := svc.Acquire(ctx, "res", "alice", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	r1, err := svc.Renew(ctx, "res", "alice", l.Token, "req-1", time.Minute)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	// 相同请求号 + 相同内容：幂等重放，返回首次结果。
	r2, err := svc.Renew(ctx, "res", "alice", l.Token, "req-1", time.Minute)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if !r1.ExpiresAt.Equal(r2.ExpiresAt) {
		t.Fatalf("replay changed result: %v vs %v", r1.ExpiresAt, r2.ExpiresAt)
	}
	// 相同请求号 + 不同内容：冲突。
	if _, err := svc.Renew(ctx, "res", "alice", l.Token, "req-1", 2*time.Minute); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting replay: err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestReleaseIdempotencyAndStaleHolder(t *testing.T) {
	clock := newFakeClock()
	svc, _ := openTest(t, clock)
	ctx := context.Background()

	l1, err := svc.Acquire(ctx, "res", "alice", 10*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := svc.Release(ctx, "res", "alice", l1.Token, "rel-1"); err != nil {
		t.Fatalf("release: %v", err)
	}
	// 幂等重放：相同请求号相同内容，成功且无副作用。
	if err := svc.Release(ctx, "res", "alice", l1.Token, "rel-1"); err != nil {
		t.Fatalf("idempotent release replay: %v", err)
	}
	// 相同请求号改换内容：冲突。
	if err := svc.Release(ctx, "res", "alice", l1.Token+1, "rel-1"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting release replay: err = %v, want ErrIdempotencyConflict", err)
	}

	// 新持有者获取后，旧持有者不得释放新租约。
	l2, err := svc.Acquire(ctx, "res", "bob", 10*time.Second)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if err := svc.Release(ctx, "res", "alice", l1.Token, "rel-2"); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("release by stale holder: err = %v, want ErrStaleToken", err)
	}

	// 租约过期后，迟到的释放（即使 token 曾匹配）必须失败。
	if err := svc.Release(ctx, "res", "bob", l2.Token, "rel-3"); err != nil {
		t.Fatalf("release by current holder: %v", err)
	}
	l3, err := svc.Acquire(ctx, "res", "carol", 10*time.Second)
	if err != nil {
		t.Fatalf("acquire by carol: %v", err)
	}
	clock.advance(11 * time.Second)
	if err := svc.Release(ctx, "res", "carol", l3.Token, "rel-4"); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late release after expiry: err = %v, want ErrLeaseExpired", err)
	}
}

func TestConditionalWrite(t *testing.T) {
	clock := newFakeClock()
	svc, _ := openTest(t, clock)
	ctx := context.Background()

	l1, err := svc.Acquire(ctx, "res", "alice", 10*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := svc.Write(ctx, "res", l1.Token, []byte("v1")); err != nil {
		t.Fatalf("write with valid token: %v", err)
	}

	// 旧持有者过期后，迟到写入必须失败，且不得修改数据。
	clock.advance(11 * time.Second)
	if err := svc.Write(ctx, "res", l1.Token, []byte("stale")); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late write: err = %v, want ErrLeaseExpired", err)
	}

	l2, err := svc.Acquire(ctx, "res", "bob", 10*time.Second)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	// 旧 token 对新租约而言是陈旧的。
	if err := svc.Write(ctx, "res", l1.Token, []byte("stale")); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("write with stale token: err = %v, want ErrStaleToken", err)
	}

	got, tok, err := svc.Read(ctx, "res")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "v1" || tok != l1.Token {
		t.Fatalf("failed writes changed data: value=%q token=%d", got, tok)
	}

	if err := svc.Write(ctx, "res", l2.Token, []byte("v2")); err != nil {
		t.Fatalf("write by new holder: %v", err)
	}
	got, tok, err = svc.Read(ctx, "res")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "v2" || tok != l2.Token {
		t.Fatalf("data = %q (token %d), want v2 (token %d)", got, tok, l2.Token)
	}
}

func TestStatus(t *testing.T) {
	clock := newFakeClock()
	svc, _ := openTest(t, clock)
	ctx := context.Background()

	st, err := svc.Status(ctx, "res")
	if err != nil {
		t.Fatalf("status of untouched resource: %v", err)
	}
	if st.Active || st.NextToken != 1 {
		t.Fatalf("fresh status = %+v", st)
	}

	l, err := svc.Acquire(ctx, "res", "alice", 10*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	st, err = svc.Status(ctx, "res")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !st.Active || st.Holder != "alice" || st.Token != l.Token || st.NextToken != l.Token+1 {
		t.Fatalf("status = %+v, lease = %+v", st, l)
	}

	clock.advance(11 * time.Second)
	st, err = svc.Status(ctx, "res")
	if err != nil {
		t.Fatalf("status after expiry: %v", err)
	}
	if st.Active {
		t.Fatalf("lease still active after expiry: %+v", st)
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	clock := newFakeClock()
	svc, path := openTest(t, clock)
	ctx := context.Background()

	l, err := svc.Acquire(ctx, "res", "alice", time.Hour)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := svc.Write(ctx, "res", l.Token, []byte("durable")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := svc.Renew(ctx, "res", "alice", l.Token, "req-1", time.Hour); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重新打开同一数据库：租约、数据、token 计数与幂等记录都必须保留。
	svc2, err := Open(path, WithClock(clock.now))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer svc2.Close()

	st, err := svc2.Status(ctx, "res")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !st.Active || st.Holder != "alice" || st.Token != l.Token {
		t.Fatalf("lease lost across reopen: %+v", st)
	}
	got, _, err := svc2.Read(ctx, "res")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "durable" {
		t.Fatalf("data lost across reopen: %q", got)
	}
	// token 永不复用：重启后继续递增。
	if err := svc2.Release(ctx, "res", "alice", l.Token, "rel-1"); err != nil {
		t.Fatalf("release: %v", err)
	}
	l2, err := svc2.Acquire(ctx, "res", "bob", time.Hour)
	if err != nil {
		t.Fatalf("acquire after reopen: %v", err)
	}
	if l2.Token != l.Token+1 {
		t.Fatalf("token reused across restart: %d then %d", l.Token, l2.Token)
	}
	// 幂等记录在重启后仍然生效。
	if _, err := svc2.Renew(ctx, "res", "alice", l.Token, "req-1", 2*time.Hour); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("idempotency lost across reopen: err = %v", err)
	}
}

func TestInvalidArguments(t *testing.T) {
	svc, _ := openTest(t, nil)
	ctx := context.Background()

	cases := []error{
		func() error { _, err := svc.Acquire(ctx, "", "h", time.Minute); return err }(),
		func() error { _, err := svc.Acquire(ctx, "r", "", time.Minute); return err }(),
		func() error { _, err := svc.Acquire(ctx, "r", "h", 0); return err }(),
		func() error { _, err := svc.Renew(ctx, "r", "h", 0, "id", time.Minute); return err }(),
		func() error { _, err := svc.Renew(ctx, "r", "h", 1, "", time.Minute); return err }(),
		func() error { return svc.Release(ctx, "r", "h", 1, "") }(),
		func() error { return svc.Write(ctx, "", 1, nil) }(),
		func() error { return svc.Write(ctx, "r", 0, nil) }(),
		func() error { _, err := svc.Status(ctx, ""); return err }(),
	}
	for i, err := range cases {
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("case %d: err = %v, want ErrInvalidArgument", i, err)
		}
	}
}

// TestConcurrentOperations 并发压测获取/续约/释放/条件写入，
// 验证不变量：任一时刻至多一个有效持有者、token 单调且不复用、
// 失败操作不改变资源数据。
func TestConcurrentOperations(t *testing.T) {
	svc, _ := openTest(t, nil)
	ctx := context.Background()

	const workers = 16
	const rounds = 25

	var wg sync.WaitGroup
	tokens := make(chan int64, workers*rounds)
	errs := make(chan error, workers*rounds)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			holder := fmt.Sprintf("holder-%d", id)
			for i := 0; i < rounds; i++ {
				l, err := svc.Acquire(ctx, "shared", holder, 50*time.Millisecond)
				if err != nil {
					if !errors.Is(err, ErrBusy) {
						errs <- fmt.Errorf("acquire: %w", err)
					}
					time.Sleep(time.Millisecond)
					continue
				}
				tokens <- l.Token

				// 持有时写入必须成功。
				if err := svc.Write(ctx, "shared", l.Token, []byte(holder)); err != nil {
					errs <- fmt.Errorf("write by valid holder: %w", err)
				}
				// 续约与释放都允许因并发过期而失败，但不允许其它错误。
				if _, err := svc.Renew(ctx, "shared", holder, l.Token, fmt.Sprintf("rn-%d-%d", id, i), 50*time.Millisecond); err != nil &&
					!errors.Is(err, ErrLeaseExpired) && !errors.Is(err, ErrStaleToken) {
					errs <- fmt.Errorf("renew: %w", err)
				}
				if err := svc.Release(ctx, "shared", holder, l.Token, fmt.Sprintf("rl-%d-%d", id, i)); err != nil &&
					!errors.Is(err, ErrLeaseExpired) && !errors.Is(err, ErrStaleToken) {
					errs <- fmt.Errorf("release: %w", err)
				}
			}
		}(w)
	}
	wg.Wait()
	close(tokens)
	close(errs)

	for err := range errs {
		t.Errorf("unexpected error: %v", err)
	}

	// 所有成功获取的 token 必须互不相同（单调计数器保证不复用）。
	seen := map[int64]bool{}
	var maxToken int64
	for tok := range tokens {
		if seen[tok] {
			t.Errorf("token %d issued twice", tok)
		}
		seen[tok] = true
		if tok > maxToken {
			maxToken = tok
		}
	}

	st, err := svc.Status(ctx, "shared")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.NextToken != maxToken+1 {
		t.Errorf("next token = %d, want %d (max issued %d)", st.NextToken, maxToken+1, maxToken)
	}

	// 资源数据必须是由某个有效 token 写入的。
	_, dataToken, err := svc.Read(ctx, "shared")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if dataToken < 1 || dataToken > maxToken {
		t.Errorf("data written by unknown token %d (max %d)", dataToken, maxToken)
	}
}
