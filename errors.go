package fencinglock

import "errors"

// 租约服务返回的可判定错误。
var (
	// ErrNotFound 表示资源或租约不存在，或从未被申请过。
	ErrNotFound = errors.New("fencinglock: lease not found")
	// ErrConflict 表示幂等请求号被复用，但请求内容与首次不一致。
	ErrConflict = errors.New("fencinglock: request id reused with different content")
	// ErrUnavailable 表示资源当前被其他持有者占用（或租约未过期），
	// 组合申请中任一资源不可用都会返回该错误，且不会产生任何可见变更。
	ErrUnavailable = errors.New("fencinglock: resource unavailable")
	// ErrInvalidRequest 表示请求参数非法，例如空资源集合或空持有者。
	ErrInvalidRequest = errors.New("fencinglock: invalid request")
	// ErrNotHolder 表示续租或释放操作的持有者与当前持有者不一致。
	ErrNotHolder = errors.New("fencinglock: not the current holder")
	// ErrInvalidToken 表示受保护写入携带的 fencing token 已过期或错误。
	ErrInvalidToken = errors.New("fencinglock: invalid fencing token")
	// ErrInvalidGroup 表示组合操作的版本号错误，或资源不属于该组合。
	ErrInvalidGroup = errors.New("fencinglock: invalid composite lease version")
	// ErrInvalidSet 表示组合写入携带的资源集合与组合持有的集合不一致。
	ErrInvalidSet = errors.New("fencinglock: resource set does not match composite lease")
	// ErrSplitGroup 表示试图对仍属于有效组合租约的单个资源做单资源操作。
	ErrSplitGroup = errors.New("fencinglock: resource is part of a composite lease")
)
