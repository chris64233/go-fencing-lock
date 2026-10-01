# go-fencing-lock

用于承载分布式资源租约与所有权管理相关的 Go 服务代码。

开发环境：Go 1.23.0。

运行测试：

    go test ./...

## 功能概述

`Service` 提供带围栏版本（fencing token）的租约锁，支持持有者交接：

- `Acquire(lock, holder, ttl)`：获取租约，返回递增的围栏版本；租约过期后重新获取会继续递增版本。
- `Write(lock, holder, version, data)`：携带围栏版本写入资源，只有当前版本可以修改资源。
- `RequestTransfer(req)` / `ConfirmTransfer(req)`：两阶段持有者交接。
- `Inspect(lock)`：查询锁当前状态与完整交接时间线。

## 持有者交接

转移请求 `TransferRequest` 携带锁名、当前版本、目标持有者与交接期限（`Deadline`）：

1. `RequestTransfer` 校验版本与租约后登记待确认交接。
2. 交接期限内调用 `ConfirmTransfer` 确认：旧持有者任期以 `transferred` 关闭，
   目标持有者获得递增的围栏版本与新租约。
3. 超过交接期限确认返回 `ErrTransferExpired`，原租约继续按原期限有效。

旧持有者写入、转移确认与租约过期并发到达时，只有当前版本可以修改资源；
交接完成后旧持有者即使本地租约尚未过期，写入也只会得到 `StaleVersionError`，
错误中明确携带旧版本（`Got`）与当前版本（`Current`）。

## 幂等与冲突

转移请求通过 `RequestID` 幂等：相同请求重复确认返回原结果，版本不重复递增；
相同 `RequestID` 但锁名、版本或目标持有者变化时返回 `TransferConflictError`。

## 时间线

`Inspect` 返回的 `History` 记录每次持有者任期：版本、持有者、获取时间、
失效时间与失效原因（`transferred` / `expired`）。旧版本写入的拒绝只返回错误，
不会被记入时间线，因此同一版本的拒绝不会被重复记成多次交接。
资源写入失败不影响状态查询，管理员可依据时间线判断交接发生的先后。
