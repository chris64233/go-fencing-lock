# go-fencing-lock

持久化的资源租约服务，为分布式场景提供带 **fencing token** 的独占租约与受保护条件写入。

开发环境：Go 1.23.0。存储使用纯 Go 的 SQLite 驱动（`modernc.org/sqlite`），无 CGO 依赖。

## 特性

- **独占租约**：客户端按资源申请有期限的租约；租约未过期时被他人获取返回 `ErrBusy`。
- **Fencing token**：每个资源维护持久化的单调计数器，每一次全新的成功获取都签发严格递增、
  永不复用的 token——释放、过期、进程重启都不会导致 token 回退或重用。
- **持有者校验**：续约（`Renew`）与释放（`Release`）要求持有者身份和 token 同时匹配，
  且租约未过期；租约一旦过期，旧持有者即使迟到也无法续约、释放或写入。
- **幂等语义**：续约与释放以请求号（request ID）幂等。同一请求号重放相同内容返回首次的
  结果；改换内容则返回 `ErrIdempotencyConflict`。幂等记录同样持久化，重启后仍然生效。
- **受保护条件写入**：`Write` 在同一个 SQLite 事务内校验"当前有效租约的 token == 请求
  token"并写入业务数据——校验与写入处于同一持久化边界，不依赖进程内锁，过期或陈旧的
  token 无法修改资源数据，失败的写入不产生任何副作用。
- **并发安全**：获取、续约、释放、条件写入可任意并发；SQLite 事务（`BEGIN IMMEDIATE` +
  WAL + busy timeout）保证每个资源任一时刻至多一个有效持有者。

## API 概览

```go
svc, err := fencinglock.Open("leases.db") // ":memory:" 可得纯内存实例
defer svc.Close()

// 申请租约：租约被持有且未过期时返回 ErrBusy
lease, err := svc.Acquire(ctx, "orders", "worker-1", 10*time.Second)

// 续约 / 释放：以请求号幂等；token 不匹配返回 ErrStaleToken，已过期返回 ErrLeaseExpired
lease, err = svc.Renew(ctx, "orders", "worker-1", lease.Token, "req-42", 10*time.Second)
err = svc.Release(ctx, "orders", "worker-1", lease.Token, "req-43")

// 受保护条件写入：仅当 token 等于当前有效租约的 token 时生效
err = svc.Write(ctx, "orders", lease.Token, []byte(`{"state":"processing"}`))

// 查询资源数据与租约状态
value, token, err := svc.Read(ctx, "orders")
status, err := svc.Status(ctx, "orders") // Active / Holder / Token / NextToken / ExpiresAt
```

## 错误分类

所有错误均为 `sentinel error` 的包装，用 `errors.Is` 判断：

| 错误 | 含义 |
| --- | --- |
| `ErrBusy` | 资源被他人持有且租约未过期 |
| `ErrLeaseExpired` | 租约已过期（或不存在有效租约） |
| `ErrStaleToken` | 请求 token 与当前租约不匹配 |
| `ErrIdempotencyConflict` | 同一请求号以不同内容重放 |
| `ErrInvalidArgument` | 参数非法（空资源名/持有者、非正 TTL 或 token 等） |

## 存储

单个 SQLite 数据库文件，四张表：

- `fencing_counters`：每资源的下一个 token（单调递增，永不复用）；
- `leases`：每资源至多一行的当前租约（持有者、token、到期时间）；
- `idempotency_keys`：请求号 → 请求哈希 + 响应，支撑幂等与冲突检测；
- `resource_data`：受保护的业务数据及写入时的 token。

所有"校验 + 修改"均在同一事务内完成并持久化，进程崩溃或重启后状态不丢失。

## 测试

    go test ./...          # 单元测试（注入假时钟，确定性验证到期行为）
    go test -race ./...    # 含并发压测：多 goroutine 竞争获取/续约/释放/写入，
                           # 验证单持有者、token 单调、失败无副作用等不变量
