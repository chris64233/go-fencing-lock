package fencinglock

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	_ "modernc.org/sqlite"
)

// MaxTTL 是单个租约期限允许的最大值。
const MaxTTL = time.Hour

// Lease 描述一次成功获取或续约后的租约。
type Lease struct {
	Resource  string
	Holder    string
	Token     int64
	ExpiresAt time.Time
}

// Status 是资源的只读快照：当前租约与受保护数据的元信息。
type Status struct {
	Resource string
	Held     bool // 是否存在未过期的租约
	Holder   string
	Token    int64
	// ExpiresAt 仅在存在租约记录时有意义（可能已过期）。
	ExpiresAt time.Time
	// DataVersion 是受保护数据的版本号，每次成功写入递增；0 表示从未写入。
	DataVersion int64
	// DataToken 是最后一次成功写入时使用的 fencing token。
	DataToken int64
}

// Service 是持久化的租约服务。所有变更操作都在同一个 SQLite
// 事务内完成"校验 fencing token + 修改状态"，因此 token 校验
// 发生在持久化边界上，而不是仅依赖进程内锁。
type Service struct {
	db  *sql.DB
	now func() time.Time
}

// Option 自定义 Service 行为。
type Option func(*Service)

// WithClock 替换时间来源，主要用于测试租约到期行为。
func WithClock(now func() time.Time) Option {
	return func(s *Service) { s.now = now }
}

// Open 打开（必要时创建）位于 path 的租约服务。path 传 ":memory:"
// 时使用纯内存数据库（不持久化，仅适合测试）。
func Open(path string, opts ...Option) (*Service, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: empty database path", ErrInvalidArgument)
	}
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("fencinglock: open database: %w", err)
	}
	// SQLite 同一时刻只允许一个写事务；单连接让事务天然串行，
	// 避免上层看到 SQLITE_BUSY。
	db.SetMaxOpenConns(1)
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	s := &Service{db: db, now: time.Now}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Close 关闭底层数据库。
func (s *Service) Close() error { return s.db.Close() }

func migrate(db *sql.DB) error {
	stmts := []string{
		// 全局 fencing token 计数器，持久化保证重启后仍单调递增、永不复用。
		`CREATE TABLE IF NOT EXISTS meta (
			key   TEXT PRIMARY KEY,
			value INTEGER NOT NULL
		)`,
		`INSERT OR IGNORE INTO meta(key, value) VALUES ('next_token', 1)`,
		// 每个资源至多一行，由主键保证"至多一个持有者"。
		`CREATE TABLE IF NOT EXISTS leases (
			resource   TEXT PRIMARY KEY,
			holder     TEXT NOT NULL,
			token      INTEGER NOT NULL,
			expires_at INTEGER NOT NULL,
			acquired_at INTEGER NOT NULL,
			renewed_at INTEGER NOT NULL
		)`,
		// 幂等请求表：请求号 -> 请求内容哈希与成功结果。
		`CREATE TABLE IF NOT EXISTS requests (
			request_id   TEXT PRIMARY KEY,
			op           TEXT NOT NULL,
			payload_hash TEXT NOT NULL,
			result       TEXT NOT NULL
		)`,
		// 受保护的业务数据。
		`CREATE TABLE IF NOT EXISTS resource_data (
			resource   TEXT PRIMARY KEY,
			data       BLOB NOT NULL,
			version    INTEGER NOT NULL,
			token      INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("fencinglock: migrate: %w", err)
		}
	}
	return nil
}

// storedResult 是记录进幂等表的成功结果。
type storedResult struct {
	Lease   *Lease `json:"lease,omitempty"`
	Version int64  `json:"version,omitempty"`
}

// Acquire 为 holder 申请 resource 上期限为 ttl 的独占租约。
// 每一次全新的成功获取都会分配一个严格递增、永不复用的 fencing
// token。资源被他人持有且未过期时返回 ErrBusy。相同 requestID
// 的重试返回首次获取的结果，不会分配新 token。
func (s *Service) Acquire(ctx context.Context, resource, holder string, ttl time.Duration, requestID string) (Lease, error) {
	if err := validateAcquire(resource, holder, ttl, requestID); err != nil {
		return Lease{}, err
	}
	hash := payloadHash("acquire", resource, holder, 0, ttl, nil)
	var out Lease
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		if res, ok, err := checkRequest(ctx, tx, requestID, hash); err != nil || ok {
			if err == nil {
				out = *res.Lease
			}
			return err
		}

		now := s.now()
		holder0, expires0, ok, err := s.loadLease(ctx, tx, resource)
		if err != nil {
			return err
		}
		if ok && now.Before(expires0) {
			return fmt.Errorf("%w: resource %q is held by %q until %s",
				ErrBusy, resource, holder0, expires0.UTC().Format(time.RFC3339Nano))
		}

		token, err := nextToken(ctx, tx)
		if err != nil {
			return err
		}
		expires := now.Add(ttl)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO leases(resource, holder, token, expires_at, acquired_at, renewed_at)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(resource) DO UPDATE SET
				holder = excluded.holder,
				token = excluded.token,
				expires_at = excluded.expires_at,
				acquired_at = excluded.acquired_at,
				renewed_at = excluded.renewed_at`,
			resource, holder, token, expires.UnixNano(), now.UnixNano(), now.UnixNano()); err != nil {
			return fmt.Errorf("fencinglock: persist lease: %w", err)
		}

		out = Lease{Resource: resource, Holder: holder, Token: token, ExpiresAt: expires}
		return recordRequest(ctx, tx, requestID, "acquire", hash, storedResult{Lease: &out})
	})
	return out, err
}

// Renew 由持有者凭当前 fencing token 续约，租约期从当前时刻
// 延长 ttl。租约已过期返回 ErrLeaseExpired，token 不匹配返回
// ErrStaleToken。按 requestID 幂等。
func (s *Service) Renew(ctx context.Context, resource, holder string, token int64, ttl time.Duration, requestID string) (Lease, error) {
	if err := validateHolderOp(resource, holder, token, requestID); err != nil {
		return Lease{}, err
	}
	if ttl <= 0 || ttl > MaxTTL {
		return Lease{}, fmt.Errorf("%w: ttl must be in (0, %s]", ErrInvalidArgument, MaxTTL)
	}
	hash := payloadHash("renew", resource, holder, token, ttl, nil)
	var out Lease
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		if res, ok, err := checkRequest(ctx, tx, requestID, hash); err != nil || ok {
			if err == nil {
				out = *res.Lease
			}
			return err
		}

		now := s.now()
		holder0, token0, _, err := s.currentLease(ctx, tx, resource, now)
		if err != nil {
			return err
		}
		if token != token0 {
			return fmt.Errorf("%w: resource %q current token is %d, got %d",
				ErrStaleToken, resource, token0, token)
		}
		if holder != holder0 {
			return fmt.Errorf("%w: resource %q is held by %q, not %q",
				ErrNotHolder, resource, holder0, holder)
		}

		expires := now.Add(ttl)
		if _, err := tx.ExecContext(ctx,
			`UPDATE leases SET expires_at = ?, renewed_at = ? WHERE resource = ?`,
			expires.UnixNano(), now.UnixNano(), resource); err != nil {
			return fmt.Errorf("fencinglock: persist renew: %w", err)
		}

		out = Lease{Resource: resource, Holder: holder, Token: token, ExpiresAt: expires}
		return recordRequest(ctx, tx, requestID, "renew", hash, storedResult{Lease: &out})
	})
	return out, err
}

// Release 由持有者凭当前 fencing token 释放租约。租约已过期
// （包括被后来者重新获取）时返回错误，旧持有者无法释放他人的
// 租约。按 requestID 幂等。
func (s *Service) Release(ctx context.Context, resource, holder string, token int64, requestID string) error {
	if err := validateHolderOp(resource, holder, token, requestID); err != nil {
		return err
	}
	hash := payloadHash("release", resource, holder, token, 0, nil)
	return s.inTx(ctx, func(tx *sql.Tx) error {
		if _, ok, err := checkRequest(ctx, tx, requestID, hash); err != nil || ok {
			return err
		}

		now := s.now()
		holder0, token0, _, err := s.currentLease(ctx, tx, resource, now)
		if err != nil {
			return err
		}
		if token != token0 {
			return fmt.Errorf("%w: resource %q current token is %d, got %d",
				ErrStaleToken, resource, token0, token)
		}
		if holder != holder0 {
			return fmt.Errorf("%w: resource %q is held by %q, not %q",
				ErrNotHolder, resource, holder0, holder)
		}

		if _, err := tx.ExecContext(ctx, `DELETE FROM leases WHERE resource = ?`, resource); err != nil {
			return fmt.Errorf("fencinglock: persist release: %w", err)
		}
		return recordRequest(ctx, tx, requestID, "release", hash, storedResult{})
	})
}

// WriteProtected 是受保护的条件写入：在同一个持久化事务内校验
// token 与当前有效租约一致后才写入业务数据，校验失败则数据保持
// 不变。成功后数据版本号递增。按 requestID 幂等。
func (s *Service) WriteProtected(ctx context.Context, resource string, token int64, data []byte, requestID string) (int64, error) {
	if resource == "" {
		return 0, fmt.Errorf("%w: empty resource", ErrInvalidArgument)
	}
	if token <= 0 {
		return 0, fmt.Errorf("%w: token must be positive", ErrInvalidArgument)
	}
	if requestID == "" {
		return 0, fmt.Errorf("%w: empty request id", ErrInvalidArgument)
	}
	hash := payloadHash("write", resource, "", token, 0, data)
	var version int64
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		if res, ok, err := checkRequest(ctx, tx, requestID, hash); err != nil || ok {
			if err == nil {
				version = res.Version
			}
			return err
		}

		now := s.now()
		_, token0, _, err := s.currentLease(ctx, tx, resource, now)
		if err != nil {
			return err
		}
		if token != token0 {
			return fmt.Errorf("%w: resource %q current token is %d, got %d",
				ErrStaleToken, resource, token0, token)
		}

		if _, err := tx.ExecContext(ctx, `
			INSERT INTO resource_data(resource, data, version, token, updated_at)
			VALUES (?, ?, 1, ?, ?)
			ON CONFLICT(resource) DO UPDATE SET
				data = excluded.data,
				version = resource_data.version + 1,
				token = excluded.token,
				updated_at = excluded.updated_at`,
			resource, data, token, now.UnixNano()); err != nil {
			return fmt.Errorf("fencinglock: persist protected write: %w", err)
		}
		if err := tx.QueryRowContext(ctx,
			`SELECT version FROM resource_data WHERE resource = ?`, resource).Scan(&version); err != nil {
			return fmt.Errorf("fencinglock: read back version: %w", err)
		}
		return recordRequest(ctx, tx, requestID, "write", hash, storedResult{Version: version})
	})
	return version, err
}

// Status 返回资源当前租约与受保护数据的只读快照。
func (s *Service) Status(ctx context.Context, resource string) (Status, error) {
	if resource == "" {
		return Status{}, fmt.Errorf("%w: empty resource", ErrInvalidArgument)
	}
	st := Status{Resource: resource}
	now := s.now()
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		var expiresNano int64
		err := tx.QueryRowContext(ctx,
			`SELECT holder, token, expires_at FROM leases WHERE resource = ?`, resource).
			Scan(&st.Holder, &st.Token, &expiresNano)
		switch {
		case err == nil:
			st.ExpiresAt = time.Unix(0, expiresNano)
			st.Held = now.Before(st.ExpiresAt)
		case errors.Is(err, sql.ErrNoRows):
		default:
			return err
		}
		var version, token int64
		err = tx.QueryRowContext(ctx,
			`SELECT version, token FROM resource_data WHERE resource = ?`, resource).Scan(&version, &token)
		switch {
		case err == nil:
			st.DataVersion, st.DataToken = version, token
		case errors.Is(err, sql.ErrNoRows):
		default:
			return err
		}
		return nil
	})
	return st, err
}

// ReadData 读取受保护的业务数据及其版本信息。
func (s *Service) ReadData(ctx context.Context, resource string) (data []byte, version int64, token int64, err error) {
	if resource == "" {
		return nil, 0, 0, fmt.Errorf("%w: empty resource", ErrInvalidArgument)
	}
	err = s.inTx(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx,
			`SELECT data, version, token FROM resource_data WHERE resource = ?`, resource).
			Scan(&data, &version, &token)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: no data for resource %q", ErrNotFound, resource)
		}
		return err
	})
	return data, version, token, err
}

// loadLease 读取租约行（可能已过期）。ok 为 false 表示从未获取或已释放。
func (s *Service) loadLease(ctx context.Context, tx *sql.Tx, resource string) (holder string, expires time.Time, ok bool, err error) {
	var expiresNano int64
	err = tx.QueryRowContext(ctx,
		`SELECT holder, expires_at FROM leases WHERE resource = ?`, resource).
		Scan(&holder, &expiresNano)
	if errors.Is(err, sql.ErrNoRows) {
		return "", time.Time{}, false, nil
	}
	if err != nil {
		return "", time.Time{}, false, err
	}
	return holder, time.Unix(0, expiresNano), true, nil
}

// currentLease 读取租约并强制其未过期，是续约/释放/受保护写入
// 共用的过期闸门：过期或不存在一律报 ErrLeaseExpired。
func (s *Service) currentLease(ctx context.Context, tx *sql.Tx, resource string, now time.Time) (holder string, token int64, expires time.Time, err error) {
	var expiresNano int64
	err = tx.QueryRowContext(ctx,
		`SELECT holder, token, expires_at FROM leases WHERE resource = ?`, resource).
		Scan(&holder, &token, &expiresNano)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, time.Time{}, fmt.Errorf("%w: no active lease for resource %q", ErrLeaseExpired, resource)
	}
	if err != nil {
		return "", 0, time.Time{}, err
	}
	expires = time.Unix(0, expiresNano)
	if !now.Before(expires) {
		return "", 0, time.Time{}, fmt.Errorf("%w: lease for resource %q expired at %s",
			ErrLeaseExpired, resource, expires.UTC().Format(time.RFC3339Nano))
	}
	return holder, token, expires, nil
}

// nextToken 在事务内分配全局单调递增的 fencing token。
func nextToken(ctx context.Context, tx *sql.Tx) (int64, error) {
	var token int64
	err := tx.QueryRowContext(ctx,
		`UPDATE meta SET value = value + 1 WHERE key = 'next_token' RETURNING value - 1`).Scan(&token)
	if err != nil {
		return 0, fmt.Errorf("fencinglock: allocate fencing token: %w", err)
	}
	return token, nil
}

// checkRequest 查询幂等表。命中且内容一致时返回已记录的结果
// （ok=true）；命中但内容不一致时返回 ErrIdempotencyConflict。
func checkRequest(ctx context.Context, tx *sql.Tx, requestID, hash string) (res storedResult, ok bool, err error) {
	var storedHash, storedResultJSON string
	err = tx.QueryRowContext(ctx,
		`SELECT payload_hash, result FROM requests WHERE request_id = ?`, requestID).
		Scan(&storedHash, &storedResultJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return storedResult{}, false, nil
	}
	if err != nil {
		return storedResult{}, false, err
	}
	if storedHash != hash {
		return storedResult{}, false, fmt.Errorf("%w: request id %q", ErrIdempotencyConflict, requestID)
	}
	if err := json.Unmarshal([]byte(storedResultJSON), &res); err != nil {
		return storedResult{}, false, fmt.Errorf("fencinglock: decode stored result: %w", err)
	}
	return res, true, nil
}

// recordRequest 把成功结果写入幂等表，与业务变更同事务提交。
func recordRequest(ctx context.Context, tx *sql.Tx, requestID, op, hash string, res storedResult) error {
	raw, err := json.Marshal(res)
	if err != nil {
		return fmt.Errorf("fencinglock: encode result: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO requests(request_id, op, payload_hash, result) VALUES (?, ?, ?, ?)`,
		requestID, op, hash, string(raw)); err != nil {
		return fmt.Errorf("fencinglock: record request: %w", err)
	}
	return nil
}

// inTx 在单个 IMMEDIATE 事务中执行 fn，提交失败时回滚，
// 保证失败操作不留下任何持久化变更。
func (s *Service) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("fencinglock: begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("fencinglock: commit: %w", err)
	}
	return nil
}

func validateAcquire(resource, holder string, ttl time.Duration, requestID string) error {
	if resource == "" {
		return fmt.Errorf("%w: empty resource", ErrInvalidArgument)
	}
	if holder == "" {
		return fmt.Errorf("%w: empty holder", ErrInvalidArgument)
	}
	if ttl <= 0 || ttl > MaxTTL {
		return fmt.Errorf("%w: ttl must be in (0, %s]", ErrInvalidArgument, MaxTTL)
	}
	if requestID == "" {
		return fmt.Errorf("%w: empty request id", ErrInvalidArgument)
	}
	return nil
}

func validateHolderOp(resource, holder string, token int64, requestID string) error {
	if resource == "" {
		return fmt.Errorf("%w: empty resource", ErrInvalidArgument)
	}
	if holder == "" {
		return fmt.Errorf("%w: empty holder", ErrInvalidArgument)
	}
	if token <= 0 {
		return fmt.Errorf("%w: token must be positive", ErrInvalidArgument)
	}
	if requestID == "" {
		return fmt.Errorf("%w: empty request id", ErrInvalidArgument)
	}
	return nil
}

// payloadHash 对操作的全部业务参数做哈希，用于识别"同一请求号
// 换了内容"的幂等冲突。
func payloadHash(op, resource, holder string, token int64, ttl time.Duration, data []byte) string {
	h := sha256.New()
	for _, part := range []string{op, resource, holder, strconv.FormatInt(token, 10), strconv.FormatInt(int64(ttl), 10)} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}
