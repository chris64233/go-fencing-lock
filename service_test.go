package fencinglock

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(1_700_000_000, 0)}
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

func newTestService() (*Service, *fakeClock) {
	clock := newFakeClock()
	svc := NewService()
	svc.now = clock.now
	return svc, clock
}

func transferReq(id, lock string, version uint64, target string, deadline time.Time) TransferRequest {
	return TransferRequest{
		RequestID:    id,
		LockName:     lock,
		Version:      version,
		TargetHolder: target,
		Deadline:     deadline,
		LeaseTTL:     time.Minute,
	}
}

func TestAcquireAndWrite(t *testing.T) {
	svc, _ := newTestService()

	grant, err := svc.Acquire("res", "alice", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if grant.Version != 1 {
		t.Fatalf("version = %d, want 1", grant.Version)
	}
	if err := svc.Write("res", "alice", grant.Version, "v1-data"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := svc.Acquire("res", "bob", time.Minute); !errors.Is(err, ErrLockHeld) {
		t.Fatalf("second acquire err = %v, want ErrLockHeld", err)
	}
	info, err := svc.Inspect("res")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if info.Data != "v1-data" || info.Holder != "alice" {
		t.Fatalf("unexpected state: %+v", info)
	}
}

func TestStaleWriteAfterHandoverReturnsBothVersions(t *testing.T) {
	svc, clock := newTestService()

	grant, err := svc.Acquire("res", "alice", time.Hour)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := svc.Write("res", "alice", grant.Version, "alice-data"); err != nil {
		t.Fatalf("write: %v", err)
	}

	req := transferReq("t1", "res", grant.Version, "bob", clock.now().Add(time.Minute))
	if err := svc.RequestTransfer(req); err != nil {
		t.Fatalf("request transfer: %v", err)
	}
	newGrant, err := svc.ConfirmTransfer(req)
	if err != nil {
		t.Fatalf("confirm transfer: %v", err)
	}
	if newGrant.Version != grant.Version+1 || newGrant.Holder != "bob" {
		t.Fatalf("unexpected grant: %+v", newGrant)
	}

	// 旧持有者本地租约未过期（1 小时），但迟到写入必须被拒绝。
	err = svc.Write("res", "alice", grant.Version, "stale-data")
	var stale *StaleVersionError
	if !errors.As(err, &stale) {
		t.Fatalf("stale write err = %v, want StaleVersionError", err)
	}
	if stale.Got != grant.Version || stale.Current != newGrant.Version {
		t.Fatalf("stale error versions = got %d current %d, want %d/%d",
			stale.Got, stale.Current, grant.Version, newGrant.Version)
	}

	info, err := svc.Inspect("res")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if info.Data != "alice-data" {
		t.Fatalf("stale write modified resource: %q", info.Data)
	}
	if err := svc.Write("res", "bob", newGrant.Version, "bob-data"); err != nil {
		t.Fatalf("new holder write: %v", err)
	}
}

func TestHandoverRaceOnlyCurrentVersionWrites(t *testing.T) {
	svc, clock := newTestService()

	grant, err := svc.Acquire("res", "alice", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	req := transferReq("t1", "res", grant.Version, "bob", clock.now().Add(time.Minute))
	req.LeaseTTL = 2 * time.Hour // 新租约需覆盖测试中的时钟前进
	if err := svc.RequestTransfer(req); err != nil {
		t.Fatalf("request transfer: %v", err)
	}

	// 旧持有者写入、转移确认、租约过期并发到达。
	var wg sync.WaitGroup
	var staleWrites, expiredWrites int64
	var confirmErr error
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := svc.Write("res", "alice", grant.Version, fmt.Sprintf("alice-%d", i))
			var stale *StaleVersionError
			var expired *LeaseExpiredError
			switch {
			case errors.As(err, &stale):
				atomic.AddInt64(&staleWrites, 1)
			case errors.As(err, &expired):
				atomic.AddInt64(&expiredWrites, 1)
			case err != nil:
				t.Errorf("unexpected write error: %v", err)
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		clock.advance(time.Hour) // 让原租约过期
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, confirmErr = svc.ConfirmTransfer(req)
	}()
	wg.Wait()

	info, err := svc.Inspect("res")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}

	// 竞态只有两种合法结局：确认先于过期（bob 接管 v2），
	// 或过期先于确认（交接失败，alice 的 v1 租约已失效）。
	// 两种结局下旧版本都不能再修改资源。
	if confirmErr == nil {
		if info.Holder != "bob" || info.Version != grant.Version+1 {
			t.Fatalf("unexpected state after race: holder %q version %d", info.Holder, info.Version)
		}
		for i := 0; i < 8; i++ {
			err := svc.Write("res", "alice", grant.Version, "late")
			var stale *StaleVersionError
			if !errors.As(err, &stale) || stale.Current != info.Version {
				t.Fatalf("late write err = %v, want StaleVersionError with current %d", err, info.Version)
			}
		}
		if err := svc.Write("res", "bob", info.Version, "bob-final"); err != nil {
			t.Fatalf("current version write: %v", err)
		}
	} else {
		var expired *LeaseExpiredError
		if !errors.As(confirmErr, &expired) {
			t.Fatalf("confirm err = %v, want nil or LeaseExpiredError", confirmErr)
		}
		if info.Holder != "alice" || info.Version != grant.Version {
			t.Fatalf("unexpected state after expiry won race: holder %q version %d", info.Holder, info.Version)
		}
		if err := svc.Write("res", "alice", grant.Version, "late"); !errors.As(err, &expired) {
			t.Fatalf("late write err = %v, want LeaseExpiredError", err)
		}
		next, err := svc.Acquire("res", "carol", 2*time.Hour)
		if err != nil {
			t.Fatalf("re-acquire after expiry: %v", err)
		}
		if next.Version != grant.Version+1 {
			t.Fatalf("re-acquire version = %d, want %d", next.Version, grant.Version+1)
		}
		if err := svc.Write("res", "carol", next.Version, "carol-final"); err != nil {
			t.Fatalf("current version write: %v", err)
		}
	}
	t.Logf("race: confirmErr=%v, %d stale rejections, %d expired rejections", confirmErr, staleWrites, expiredWrites)
}

func TestTransferIdempotencyAndConflict(t *testing.T) {
	svc, clock := newTestService()

	grant, err := svc.Acquire("res", "alice", time.Hour)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	req := transferReq("t1", "res", grant.Version, "bob", clock.now().Add(time.Minute))
	if err := svc.RequestTransfer(req); err != nil {
		t.Fatalf("request transfer: %v", err)
	}
	first, err := svc.ConfirmTransfer(req)
	if err != nil {
		t.Fatalf("confirm transfer: %v", err)
	}

	// 相同转移请求返回原结果，版本不重复递增。
	second, err := svc.ConfirmTransfer(req)
	if err != nil {
		t.Fatalf("duplicate confirm: %v", err)
	}
	if second != first {
		t.Fatalf("duplicate confirm = %+v, want %+v", second, first)
	}
	info, _ := svc.Inspect("res")
	if info.Version != first.Version {
		t.Fatalf("version advanced on duplicate confirm: %d", info.Version)
	}

	// 锁名变化返回冲突。
	conflictLock := req
	conflictLock.LockName = "other"
	if _, err := svc.ConfirmTransfer(conflictLock); err == nil {
		t.Fatal("expected conflict for changed lock name")
	} else {
		var conflict *TransferConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("err = %v, want TransferConflictError", err)
		}
	}
	// 版本变化返回冲突。
	conflictVersion := req
	conflictVersion.Version = grant.Version + 1
	if _, err := svc.ConfirmTransfer(conflictVersion); err == nil {
		t.Fatal("expected conflict for changed version")
	} else {
		var conflict *TransferConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("err = %v, want TransferConflictError", err)
		}
	}
}

func TestTransferFailureKeepsOriginalLease(t *testing.T) {
	svc, clock := newTestService()

	grant, err := svc.Acquire("res", "alice", time.Hour)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	req := transferReq("t1", "res", grant.Version, "bob", clock.now().Add(time.Minute))
	if err := svc.RequestTransfer(req); err != nil {
		t.Fatalf("request transfer: %v", err)
	}

	// 超过交接期限才确认，转移失败。
	clock.advance(2 * time.Minute)
	if _, err := svc.ConfirmTransfer(req); !errors.Is(err, ErrTransferExpired) {
		t.Fatalf("confirm err = %v, want ErrTransferExpired", err)
	}
	// 相同失败请求重试，返回原结果。
	if _, err := svc.ConfirmTransfer(req); !errors.Is(err, ErrTransferExpired) {
		t.Fatalf("retry confirm err = %v, want ErrTransferExpired", err)
	}

	// 原租约继续按原期限有效。
	if err := svc.Write("res", "alice", grant.Version, "still-alive"); err != nil {
		t.Fatalf("original holder write after failed transfer: %v", err)
	}
	info, _ := svc.Inspect("res")
	if info.Holder != "alice" || info.Version != grant.Version || info.Data != "still-alive" {
		t.Fatalf("unexpected state after failed transfer: %+v", info)
	}
	if info.Pending != nil {
		t.Fatalf("pending transfer not cleared: %+v", info.Pending)
	}

	// 原租约到期后写入失败，锁可被重新获取且版本递增。
	clock.advance(time.Hour)
	var expired *LeaseExpiredError
	if err := svc.Write("res", "alice", grant.Version, "too-late"); !errors.As(err, &expired) {
		t.Fatalf("write after lease expiry err = %v, want LeaseExpiredError", err)
	}
	next, err := svc.Acquire("res", "carol", time.Hour)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if next.Version != grant.Version+1 {
		t.Fatalf("re-acquire version = %d, want %d", next.Version, grant.Version+1)
	}
}

func TestInspectTimelineAndRejectionNotRecorded(t *testing.T) {
	svc, clock := newTestService()

	grantV1, err := svc.Acquire("res", "alice", time.Hour)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	req := transferReq("t1", "res", grantV1.Version, "bob", clock.now().Add(time.Minute))
	if err := svc.RequestTransfer(req); err != nil {
		t.Fatalf("request transfer: %v", err)
	}
	clock.advance(10 * time.Second)
	grantV2, err := svc.ConfirmTransfer(req)
	if err != nil {
		t.Fatalf("confirm transfer: %v", err)
	}

	// 同一版本的拒绝重复发生，不能被记成多次交接。
	for i := 0; i < 5; i++ {
		var stale *StaleVersionError
		if err := svc.Write("res", "alice", grantV1.Version, "stale"); !errors.As(err, &stale) {
			t.Fatalf("stale write err = %v", err)
		}
	}

	// 交接后旧租约到期，再次易主。
	clock.advance(2 * time.Hour)
	grantV3, err := svc.Acquire("res", "carol", time.Hour)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}

	info, err := svc.Inspect("res")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if len(info.History) != 3 {
		t.Fatalf("history entries = %d, want 3 (rejections must not be recorded)", len(info.History))
	}

	want := []struct {
		version uint64
		holder  string
		reason  string
	}{
		{grantV1.Version, "alice", ReasonTransferred},
		{grantV2.Version, "bob", ReasonExpired},
		{grantV3.Version, "carol", ""},
	}
	for i, w := range want {
		entry := info.History[i]
		if entry.Version != w.version || entry.Holder != w.holder || entry.Reason != w.reason {
			t.Errorf("history[%d] = %+v, want version %d holder %q reason %q",
				i, entry, w.version, w.holder, w.reason)
		}
	}
	// 管理端可依据时间线判断交接先后。
	for i := 1; i < len(info.History); i++ {
		if info.History[i].AcquiredAt.Before(info.History[i-1].AcquiredAt) {
			t.Errorf("history not ordered by acquisition time at %d", i)
		}
	}
	if info.History[0].InvalidatedAt.IsZero() {
		t.Error("transferred entry missing invalidation time")
	}
}

func TestWriteFailureKeepsStateReadable(t *testing.T) {
	svc, clock := newTestService()

	grant, err := svc.Acquire("res", "alice", time.Hour)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	req := transferReq("t1", "res", grant.Version, "bob", clock.now().Add(time.Minute))
	if err := svc.RequestTransfer(req); err != nil {
		t.Fatalf("request transfer: %v", err)
	}
	if _, err := svc.ConfirmTransfer(req); err != nil {
		t.Fatalf("confirm transfer: %v", err)
	}

	// 资源写入失败后仍能读取当前锁状态。
	if err := svc.Write("res", "alice", grant.Version, "stale"); err == nil {
		t.Fatal("expected stale write to fail")
	}
	info, err := svc.Inspect("res")
	if err != nil {
		t.Fatalf("inspect after failed write: %v", err)
	}
	if info.Holder != "bob" || info.Version != grant.Version+1 {
		t.Fatalf("state corrupted by failed write: %+v", info)
	}
	if len(info.History) != 2 || info.History[0].Reason != ReasonTransferred {
		t.Fatalf("unexpected history: %+v", info.History)
	}
}
