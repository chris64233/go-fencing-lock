# go-fencing-lock

用于承载分布式资源租约与所有权管理相关的 Go 服务代码。

开发环境：Go 1.23.0。

运行测试：

    go test ./...

## 功能概览

基于 SQLite 的持久化租约锁服务，所有变更在单个事务内完成
"校验 fencing token + 修改状态"：

- `Acquire` / `Renew` / `Release`：租约的获取、续约与释放，每次
  成功获取分配全局单调递增、永不复用的 fencing token。
- `WriteProtected`：受保护写入，只有携带当前 token 的请求才能
  修改资源数据。
- `Status` / `ReadData` / `History`：只读查询，任何时刻（包括
  写入失败后）都可用。

## 持有者交接

`Transfer(resource, holder, token, target, ttl, requestID)` 由当前
持有者发起交接：请求携带锁名、当前 fencing token、目标持有者与
交接期限（新租约期）。确认成功后为目标持有者分配递增的新 token，
旧 token 立即失效——旧持有者即使本地租约尚未过期，其迟到写入也
只会得到 `ErrStaleToken`。该错误可解包为 `*StaleTokenError`，同时
携带请求提供的旧版本（`Provided`）与当前版本（`Current`）。

交接语义：

- 并发安全：旧持有者写入、转移确认与租约过期并发到达时，只有
  当前版本可以修改资源，数据版本连续无空洞。
- 幂等：相同 `requestID` 的重复转移请求返回首次结果，不分配新
  token；同一请求号携带不同的锁名或版本返回
  `ErrIdempotencyConflict`。
- 失败无副作用：转移失败（租约过期、token 不匹配、非持有者）时
  原租约按原期限继续有效，不产生任何持久化变更。

## 租约时间线

`History(resource)` 返回按发生先后排序的持有者任期记录，每条包含
持有者、fencing token、任期起止与失效原因（`expired` /
`released` / `transferred`）。管理员可据此判断交接发生的先后。
被拒绝的操作（如旧版本的迟到写入）不会写入时间线，同一版本的
拒绝不会被重复记成多次交接。
