// Package fencinglock 提供持久化的资源租约服务：按资源签发带 fencing token 的
// 独占租约，支持幂等的续约与释放、以当前 token 为条件的受保护写入及状态查询。
// 所有"校验 + 修改"均在同一个 SQLite 事务内完成，不依赖进程内锁保证正确性。
package fencinglock
