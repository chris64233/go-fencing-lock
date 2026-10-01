package fencinglock

import (
	"errors"
	"fmt"
)

// 服务对外报告的错误类别。所有错误均可用 errors.Is 判定，
// 并可能带有资源、持有者等上下文信息。
var (
	// ErrInvalidArgument 参数错误：资源名、持有者、请求号为空，
	// 租约期限越界，或 fencing token 非法。
	ErrInvalidArgument = errors.New("fencinglock: invalid argument")

	// ErrBusy 忙碌：资源上存在未过期的租约且被其他客户端持有。
	ErrBusy = errors.New("fencinglock: resource is busy")

	// ErrLeaseExpired 过期：租约已过期（或不存在），
	// 旧持有者迟到的续约/释放/受保护写入一律被拒绝。
	ErrLeaseExpired = errors.New("fencinglock: lease has expired")

	// ErrStaleToken 陈旧 token：请求携带的 fencing token
	// 与当前持久化的租约 token 不匹配。
	ErrStaleToken = errors.New("fencinglock: stale fencing token")

	// ErrNotHolder 调用方不是当前租约持有者。
	ErrNotHolder = errors.New("fencinglock: caller is not the lease holder")

	// ErrIdempotencyConflict 幂等冲突：同一请求号被以不同的
	// 请求内容重复使用。
	ErrIdempotencyConflict = errors.New("fencinglock: request id reused with different payload")

	// ErrNotFound 查询的资源数据不存在。
	ErrNotFound = errors.New("fencinglock: not found")
)

// StaleTokenError 描述一次因 fencing token 过时而失败的变更，
// 同时携带请求提供的旧版本与当前持久化的版本，便于调用方
// （尤其是迟到写入的旧持有者）明确知道自己落后了多少。
type StaleTokenError struct {
	Resource string
	// Provided 是请求携带的（旧）fencing token。
	Provided int64
	// Current 是拒绝时持久化的当前 fencing token。
	Current int64
}

func (e *StaleTokenError) Error() string {
	return fmt.Sprintf("%s: resource %q current token is %d, got %d",
		ErrStaleToken, e.Resource, e.Current, e.Provided)
}

// Is 让 errors.Is(err, ErrStaleToken) 对 *StaleTokenError 成立。
func (e *StaleTokenError) Is(target error) bool { return target == ErrStaleToken }
