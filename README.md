# go-fencing-lock

用于承载分布式资源租约与所有权管理相关的 Go 服务代码。

开发环境：Go 1.23.0。

运行测试：

    go test ./...

## 功能概览

服务提供两类租约：

- **单资源租约**：`Acquire` / `Renew` / `Release` / `Handoff` / `Write`。
- **组合租约**：`CompositeAcquire` / `CompositeRenew` / `CompositeRelease` /
  `CompositeHandoff` / `CompositeWrite`，用于必须同时持有一组资源才能执行
  的操作。

所有操作都在进程内通过互斥锁串行化，资源始终按编号排序处理，因此与过期扫描、
交接并发时不会发生死锁。可通过 `WithClock` 注入时钟便于测试。

## Fencing token

- 每个资源维护一条独立、单调递增的 fencing token 序列；每次授权（获取、
  交接、组合申请）都会 +1。
- 受保护写入必须携带持有者与当前 token；旧持有者交接后持有的旧 token 永远
  无法再写入。
- 组合租约在 `Tokens` 中返回整组每个资源的 token，并带一个从 1 开始、续租
  与整组交接时递增的 `Version`。

## 组合租约语义

1. 申请参数：资源集合、持有者、租约期限（TTL）、请求号。
2. 资源编号先去重，再按字典序稳定处理。
3. 申请是原子的两阶段过程：
   - 先在同一版本边界内检查每个资源是否可取得（空闲、已释放、已过期或本
     持有者自己持有的单资源租约可取得；其他持有者的有效租约不可取得）。
   - 任一资源不可取得时返回 `ErrUnavailable`，已做的临时授予全部回滚，
     外部观察不到“半组已取得”，原先仍有效的单资源租约不会被误删。
4. 整组存活期间，成员不能被单独获取、单独交接或单独释放，只能通过组合
   接口管理，避免一个资源已被旧组合释放而另一个仍被其认为有效。
5. 续租、释放、交接和写入都会重新做整组版本校验
   （`groupIntact`）：每个成员必须仍属于该组合、持有者一致、token 未前进、
   未过期。任一成员被接管或过期，旧组合对任何成员的写入都会返回
   `ErrCompositeInvalid`。
6. `CompositeRelease` 原子释放整组；`CompositeHandoff` 原子交接整组，
   每个成员各自发放新 token。

## 幂等

组合申请、续租、释放以及单资源的获取、续租、释放、交接都接受 `RequestID`
（请求号）。服务以请求号为键记录原始请求内容（归一化后的资源集合、持有者、
期限、交接来源）与结果：

- 同号且内容完全一致：返回首次执行的原结果（包括首次失败时的同一错误）。
- 同号但资源集合、持有者或期限等任一变化：返回 `ErrRequestConflict`。
- 空请求号不做幂等处理。

## 过期

- 过期判断是惰性的：每次操作都会检查截止时间；`ScanExpired` 只负责列出已
  过期资源，不修改任何状态，因此与其他操作并发安全。
- 过期租约可被任何人通过 `Handoff`（需提供原持有者）或新的申请接管，接管
  立即发放新 token 并使旧组合失效。

## 查询

- `QueryResource(name)` 返回单个资源的当前 token、持有者、所属组合 ID、
  截止时间、状态（`active` / `composite` / `expired` / `released` / `free`）
  以及失效或状态说明（`Reason`）。
- `QueryResources()` 按资源编号排序列出全部资源。
- `QueryComposite(id)` 返回组合的持有者、版本、截止时间与每个成员的状态；
  当组已被释放或任一成员已接管/过期时，`Valid` 为 false 并给出原因。

## 错误

| 错误 | 含义 |
| --- | --- |
| `ErrInvalidRequest` | 缺少必填字段或 TTL 非法 |
| `ErrUnavailable` | 至少一个资源被其他持有者有效占用 |
| `ErrLeaseExpired` | 操作所依赖的租约已过期 |
| `ErrLeaseReleased` | 租约已释放或从未授予 |
| `ErrFencingToken` | 使用了过期（非当前）fencing token |
| `ErrHolderMismatch` | 持有者身份不匹配 |
| `ErrRequestConflict` | 幂等键复用但请求内容发生变化 |
| `ErrCompositeInvalid` | 组合成员被接管、过期、释放或 token 前进 |
| `ErrCompositeNotFound` | 组合租约 ID 不存在 |
