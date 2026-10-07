# go-fencing-lock

用于承载分布式资源租约与所有权管理相关的 Go 服务代码。包 `fencinglock`
提供内存版租约锁服务 `Service`，支持单资源租约、fencing token、持有者
交接，以及需要同时持有一组资源才能执行操作的“组合租约”。

开发环境：Go 1.23.0。

运行测试（含竞态检测）：

    go test -race ./...

## 核心概念

- **fencing token**：每个资源拥有自己独立的、严格单调递增的 token。
  申请、交接都会颁发新 token；释放或过期后 token 保留不复用，下一次
  颁发继续递增。受保护写入（`Write` / `WriteGroup`）必须携带当前 token。
- **组合租约版本（GroupID）**：一次组合申请成功会原子地为整组资源
  分配同一个版本号。续租、释放和写入都必须携带正确版本；版本失效后
  （任一成员被接管、整组过期或整组释放）旧版本立即不可用。
- **失效原因**：查询接口会说明资源/组合当前为何不可用，取值为
  `never`（从未申请）、`expired`（过期）、`released`（主动释放）、
  `takenover`（持有者已被交接接管）。

## API

单资源操作：

- `Acquire(AcquireRequest)`：申请租约，返回新的 fencing token。
- `Renew(RenewRequest)`：续租；属于有效组合的资源拒绝单资源续租
  （`ErrSplitGroup`）。
- `Release(ReleaseRequest)`：释放；属于有效组合的资源拒绝单资源释放。
- `Transfer(TransferRequest)`：持有者交接，校验当前持有者并为新持有者
  颁发新 token；若资源属于组合租约，该组合版本立即整体失效。
- `Write(WriteRequest)`：受保护写入前的校验（持有者 + token）。

组合租约操作：

- `AcquireGroup(GroupRequest)`：原子申请一组资源。
- `RenewGroup(GroupRenewRequest)`：按版本续租整组，全部成员到期时间统一。
- `ReleaseGroup(GroupOpRequest)`：按版本原子释放整组。
- `WriteGroup(GroupWriteRequest)`：按版本 + 整组 token 校验写入。

扫描与查询：

- `ScanExpired()`：主动执行一次过期扫描，返回被回收的资源编号。
- `GetResource(id)`：返回资源当前 token、持有者、所属组合版本、
  是否有效及失效原因。
- `ListResources()`：按资源编号稳定顺序返回全部资源状态。
- `GetGroup(groupID)`：返回组合版本的持有者、资源集合、整组 token、
  是否有效及失效原因。
- `SetClock(fn)`：注入自定义时钟（测试过期逻辑用）。

## 组合租约语义

1. **稳定处理顺序**：申请中的资源编号先去重，再按字典序排序后处理；
   返回结果中的 `Resources` 与 `Tokens` 也使用该顺序。
2. **同一版本边界、原子可见**：申请分为“只校验、不修改”和“整体提交”
   两个阶段。任一资源被其他有效持有者占用时返回
   `*UnavailableError`（可用 `errors.Is(err, ErrUnavailable)` 判定），
   并给出具体资源编号；失败时不会暴露任何半组已取得的状态。
3. **失败回滚不误删**：校验阶段不触碰现有状态，因此失败后原先仍有效
   的单资源租约（含同持有者的）保持原样，token 不变，可继续写入和续租。
4. **整组同生共死**：
   - 任一成员被交接接管，组合版本立即失效，旧组合不能再写其它资源；
     未被交接的成员恢复为旧持有者名下的独立单资源租约。
   - 组内资源共享同一到期时间；过期按整组回收，旧版本写入、续租被拒。
   - 整组释放是原子的，不存在“一个先释放、另一个仍被旧组合认为有效”
     的窗口。
5. **并发安全**：所有状态变更都在单一互斥锁内完成，先校验后提交，
   组合申请与单资源获取、交接、过期扫描之间不存在锁顺序死锁
   （测试 `TestConcurrentOperationsNoDeadlock` 以 `-race` 覆盖）。

## 幂等

申请、续租、释放、交接都支持可选 `RequestID`：

- 同号且同内容（资源集合按去重排序后比较；持有者、期限相同）返回
  首次的原始结果（含失败结果），不会重复颁发 token 或版本号。
- 同号但资源集合、持有者或期限发生变化，返回 `ErrConflict`。
- 空 `RequestID` 表示不做幂等记录。

## 错误速查

| 错误 | 含义 |
| --- | --- |
| `ErrInvalidRequest` | 参数非法（空集合、空持有者、非正期限等） |
| `ErrUnavailable` | 资源被其他有效持有者占用（见 `UnavailableError`） |
| `ErrNotFound` | 资源或组合版本不存在 |
| `ErrNotHolder` | 持有者与当前持有者不一致 |
| `ErrInvalidToken` | 写入携带的 fencing token 已过期或错误 |
| `ErrInvalidGroup` | 组合版本不存在、已失效或已过期 |
| `ErrInvalidSet` | 组合写入的 token 集合与组内资源不一一对应 |
| `ErrSplitGroup` | 对有效组合内的资源执行单资源续租/释放/写入 |
| `ErrConflict` | 幂等请求号复用但内容发生变化 |

## 使用示例

```go
svc := fencinglock.NewService()

g, err := svc.AcquireGroup(fencinglock.GroupRequest{
    ResourceIDs: []string{"db-shard-1", "db-shard-2"},
    Holder:      "job-42",
    TTL:         30 * time.Second,
    RequestID:   "job-42-step-3",
})
if err != nil {
    // 资源不全可用：整组未生效，其他资源上的既有租约不受影响
    return err
}

if err := svc.WriteGroup(fencinglock.GroupWriteRequest{
    GroupID: g.GroupID,
    Holder:  g.Holder,
    Tokens:  g.Tokens,
}); err != nil {
    // 版本失效（接管/过期/释放）或 token 过旧时写入必须中止
    return err
}

// 操作完成后原子释放整组
return svc.ReleaseGroup(fencinglock.GroupOpRequest{
    GroupID:   g.GroupID,
    Holder:    g.Holder,
    RequestID: "job-42-step-3-done",
})
```
