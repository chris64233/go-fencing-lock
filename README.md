# go-fencing-lock

持久化的资源租约服务，为分布式场景下的资源访问提供 **fencing token** 保护。

开发环境：Go 1.23.0。持久化基于 SQLite（`modernc.org/sqlite`，纯 Go，无 cgo）。

## 核心语义

- **独占租约**：客户端按资源申请有期限（TTL，最大 1 小时）的独占租约。每个资源任意时刻至多一个有效持有者（由数据库主键与单写事务保证）。
- **fencing token**：每一次全新的成功获取都分配一个严格递增、永不复用的 token。token 计数器持久化在数据库中，进程重启后依然单调。
- **持有者操作**：租约仍有效且 token 匹配时，持有者才能续约（`Renew`）或释放（`Release`）。租约一旦过期，旧持有者迟到的续约、释放、受保护写入一律被拒绝，无法影响后来者。
- **幂等**：所有变更操作携带请求号（request ID）。同一请求号重放返回首次的结果（不重复分配 token、不重复递增版本）；同一请求号改换请求内容返回 `ErrIdempotencyConflict`。
- **受保护条件写入**：`WriteProtected` 在**同一个 SQLite 事务**内校验当前持久化的 fencing token 并写入业务数据——校验发生在持久化边界上，而非仅靠进程内锁；校验失败则数据保持不变。

## API 概览

```go
svc, err := fencinglock.Open("lease.db")        // ":memory:" 可用于测试
defer svc.Close()

ctx := context.Background()

// 获取租约（他人持有且未过期时返回 ErrBusy）
lease, err := svc.Acquire(ctx, "res-1", "holder-a", 30*time.Second, "req-1")

// 续约 / 释放（需持有者与 token 匹配、租约未过期）
lease, err = svc.Renew(ctx, "res-1", "holder-a", lease.Token, 30*time.Second, "req-2")
err = svc.Release(ctx, "res-1", "holder-a", lease.Token, "req-3")

// 受保护条件写入：token 校验与数据写入在同一事务
version, err := svc.WriteProtected(ctx, "res-1", lease.Token, []byte("payload"), "req-4")

// 状态查询与数据读取
status, err := svc.Status(ctx, "res-1")          // 持有者、token、过期时间、数据版本
data, version, token, err := svc.ReadData(ctx, "res-1")
```

## 错误分类

所有错误可用 `errors.Is` 判定：

| 错误 | 含义 |
|---|---|
| `ErrBusy` | 资源被其他客户端持有且租约未过期 |
| `ErrLeaseExpired` | 租约已过期或不存在（旧持有者迟到的操作） |
| `ErrStaleToken` | fencing token 与当前持久化的租约 token 不匹配 |
| `ErrNotHolder` | 调用方不是当前持有者 |
| `ErrIdempotencyConflict` | 同一请求号被以不同内容重复使用 |
| `ErrInvalidArgument` | 参数错误（空资源名/持有者/请求号、TTL 越界、非法 token） |
| `ErrNotFound` | 查询的资源数据不存在 |

## 持久化设计

单文件 SQLite 数据库（WAL 模式，`synchronous=FULL`），四张表：

- `meta`：全局 fencing token 计数器，分配在事务内完成，重启后不复用；
- `leases`：每资源一行（主键），记录持有者、token、过期时间；
- `requests`：幂等表，请求号 → 请求内容哈希 + 成功结果，与业务变更同事务提交；
- `resource_data`：受保护业务数据，版本号随成功写入递增。

每个变更操作都在单个 `IMMEDIATE` 事务中执行"校验 → 修改 → 记录幂等结果"，提交失败整体回滚，因此失败操作不会留下任何持久化变更。数据库连接数固定为 1，事务天然串行，上层不会看到 `SQLITE_BUSY`。

时间来源可通过 `WithClock` 注入，便于测试租约到期行为。

## 运行测试

```sh
go test ./...        # 单元测试（假时钟，确定性）
go test -race ./...  # 含并发竞争检测
```

测试覆盖：token 单调性与重启后不复用、忙碌/过期/陈旧 token/幂等冲突/参数错误分类、
过期后旧持有者的续约/释放/写入被拒绝、失败写入不改变数据、数据跨重启持久化，
以及并发获取/续约/释放/写入下"至多一个持有者、版本号等于成功写入数"的不变量。
