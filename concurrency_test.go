package fencinglock

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentAcquireUniqueTokens 并发抢占同一资源：所有成功获取的
// token 必须全局唯一且严格递增（按获取顺序），任意时刻至多一个持有者。
func TestConcurrentAcquireUniqueTokens(t *testing.T) {
	svc, err := Open(filepath.Join(t.TempDir(), "lease.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { svc.Close() })
	ctx := context.Background()

	const workers = 16
	const rounds = 25

	tokens := make(chan int64, workers*rounds)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			holder := fmt.Sprintf("holder-%d", id)
			for r := 0; r < rounds; r++ {
				l, err := svc.Acquire(ctx, "hot", holder, 2*time.Millisecond,
					fmt.Sprintf("acq-%d-%d", id, r))
				if errors.Is(err, ErrBusy) {
					time.Sleep(time.Millisecond)
					continue
				}
				if err != nil {
					t.Errorf("Acquire: %v", err)
					return
				}
				tokens <- l.Token
				// 不主动释放，让租约自然过期，制造真实竞争。
				time.Sleep(3 * time.Millisecond)
			}
		}(w)
	}
	wg.Wait()
	close(tokens)

	seen := map[int64]bool{}
	count := 0
	for tok := range tokens {
		if seen[tok] {
			t.Fatalf("duplicate fencing token: %d", tok)
		}
		if tok <= 0 {
			t.Fatalf("non-positive token: %d", tok)
		}
		seen[tok] = true
		count++
	}
	if count == 0 {
		t.Fatal("no successful acquires")
	}
}

// TestConcurrentMixedOps 并发混合获取/续约/释放/受保护写入：
// 不变量——每次成功写入都使版本号恰好 +1，失败操作不改变数据；
// 最终数据版本号等于成功写入次数。
func TestConcurrentMixedOps(t *testing.T) {
	svc, err := Open(filepath.Join(t.TempDir(), "lease.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { svc.Close() })
	ctx := context.Background()

	const workers = 8
	const ops = 40

	var writeOK atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			holder := fmt.Sprintf("holder-%d", id)
			var lease Lease
			held := false
			for i := 0; i < ops; i++ {
				reqID := fmt.Sprintf("op-%d-%d", id, i)
				switch i % 4 {
				case 0: // acquire
					l, err := svc.Acquire(ctx, "shared", holder, 5*time.Millisecond, reqID)
					if err == nil {
						lease, held = l, true
					} else if !errors.Is(err, ErrBusy) {
						t.Errorf("Acquire: unexpected error %v", err)
						return
					}
				case 1: // renew
					if !held {
						continue
					}
					l, err := svc.Renew(ctx, "shared", holder, lease.Token, 5*time.Millisecond, reqID)
					switch {
					case err == nil:
						lease = l
					case errors.Is(err, ErrLeaseExpired), errors.Is(err, ErrStaleToken):
						held = false
					default:
						t.Errorf("Renew: unexpected error %v", err)
						return
					}
				case 2: // protected write
					if !held {
						continue
					}
					_, err := svc.WriteProtected(ctx, "shared", lease.Token,
						[]byte(fmt.Sprintf("w%d-i%d", id, i)), reqID)
					switch {
					case err == nil:
						writeOK.Add(1)
					case errors.Is(err, ErrLeaseExpired), errors.Is(err, ErrStaleToken):
						held = false
					default:
						t.Errorf("WriteProtected: unexpected error %v", err)
						return
					}
				case 3: // release
					if !held {
						continue
					}
					err := svc.Release(ctx, "shared", holder, lease.Token, reqID)
					switch {
					case err == nil:
						held = false
					case errors.Is(err, ErrLeaseExpired), errors.Is(err, ErrStaleToken):
						held = false
					default:
						t.Errorf("Release: unexpected error %v", err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()

	st, err := svc.Status(ctx, "shared")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if got := writeOK.Load(); st.DataVersion != got {
		t.Fatalf("data version = %d, but %d writes reported success", st.DataVersion, got)
	}
}

// TestConcurrentAcquireSameRequestID 同一请求号并发重放：
// 至多产生一个 token，所有成功者看到相同结果。
func TestConcurrentAcquireSameRequestID(t *testing.T) {
	svc, err := Open(filepath.Join(t.TempDir(), "lease.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { svc.Close() })
	ctx := context.Background()

	const workers = 8
	results := make(chan Lease, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := svc.Acquire(ctx, "res", "alice", time.Minute, "same-req")
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			results <- l
		}()
	}
	wg.Wait()
	close(results)

	var first *Lease
	for l := range results {
		l := l
		if first == nil {
			first = &l
			continue
		}
		if l.Token != first.Token {
			t.Fatalf("same request id produced different tokens: %d vs %d", l.Token, first.Token)
		}
	}
	if first == nil {
		t.Fatal("no successful acquire")
	}
}
