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
- **共享读租约与升级**：`AcquireRead` / `RenewRead` / `ReleaseRead` 加
  `RequestUpgrade` / `CompleteUpgrade` / `CancelUpgrade`，允许多个持有者
  共享读取，并支持其中一个读持有者升级为独占写租约。

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

## 共享读租约与升级

1. 每个资源的读租约按持有者分别记录期限与读版本（`readVersion`）；独占写
   租约继续使用单调递增的 fencing token。存在有效写租约（含组合租约）时不
   能获取读租约，存在有效读租约时也不能直接获取、交接或组合申请独占租约；
   读租约永远不能执行受保护写入。
2. 任一读持有者可通过 `RequestUpgrade` 申请升级。申请会冻结当前读版本与
   读持有者集合，并在等待期间阻止新的读租约与独占授权（返回
   `ErrUpgradePending`）。只有当冻结集合中除申请者外的读租约全部释放或过
   期后，`CompleteUpgrade` 才会原子地清空读者并发放新的独占 token。申请者
   必须仍持有有效读租约——先释放自己的读租约再抢占会被拒绝
   （`ErrNoReadLease` / `ErrUpgradePending`）。
3. 读租约的释放、续租、过期扫描与升级确认都在同一把互斥锁下串行化，资源
   任意时刻只呈现一套完整所有权（读者集合或唯一独占持有者，二者不共存）。
   升级取消、完成或重新申请都会推进读版本，因此旧读版本与旧升级回执无法
   产生写租约，也不会覆盖已生成的新独占 token。
4. `CancelUpgrade` 只撤销待处理升级，不删除其他读持有者；申请者自己的读
   租约是否仍有效以其原期限为准。升级请求以 `RequestID` 作为升级号：同号
   同内容返回原结果，同号不同内容返回 `ErrRequestConflict`，已有其他升级
   号在等待时返回 `ErrUpgradeConflict`。
5. `QueryResource` 会展示当前读持有者（持有者、版本、期限）、待处理升级
   （升级号、申请者、冻结版本、冻结读者与仍在等待的读者）、独占 token 以
   及阻断原因（`Reason`）。

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

组合申请、续租、释放，单资源的获取、续租、释放、交接，以及读租约的获取、
续租、释放和升级申请/确认/取消都接受 `RequestID`（请求号）。服务以请求号
为键记录原始请求内容（归一化后的资源集合、持有者、期限、交接来源）与结果：

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
  截止时间、状态（`active` / `composite` / `shared` / `expired` /
  `released` / `free`）、当前读持有者列表、待处理升级详情以及失效或阻断
  原因（`Reason`）。
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
| `ErrUpgradePending` | 有待处理升级，阻断新的读租约与独占授权 |
| `ErrUpgradeConflict` | 升级号、申请者或冻结版本不匹配，或已有升级在等待 |
| `ErrUpgradeNotFound` | 资源上不存在待处理升级 |
| `ErrNoReadLease` | 调用者不持有该资源的有效读租约 |
