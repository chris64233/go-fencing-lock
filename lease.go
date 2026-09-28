package fencinglock

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// 租约操作可能返回的错误类别，调用方应使用 errors.Is 判断。
var (
	// ErrBusy 表示资源上仍存在未过期的租约，被其他持有者占用。
	ErrBusy = errors.New("lease busy")
	// ErrLeaseExpired 表示租约已过期（或不存在有效租约）。
	ErrLeaseExpired = errors.New("lease expired")
	// ErrStaleToken 表示请求携带的 fencing token 与当前租约不匹配。
	ErrStaleToken = errors.New("stale fencing token")
	// ErrIdempotencyConflict 表示同一请求号被以不同的请求内容重放。
	ErrIdempotencyConflict = errors.New("idempotency conflict")
	// ErrInvalidArgument 表示请求参数非法。
	ErrInvalidArgument = errors.New("invalid argument")
)

// Lease 描述一次成功获取或续约后的租约状态。
type Lease struct {
	Resource  string
	Holder    string
	Token     int64
	ExpiresAt time.Time
}

// Status 描述资源当前的租约与 fencing token 状态。
type Status struct {
	Resource string
	Active   bool
	Holder   string
	Token    int64
	// NextToken 是该资源下一次成功获取将签发的 token，严格递增、永不复用。
	NextToken int64
	ExpiresAt time.Time
}

// Service 是持久化的租约服务，为资源提供带 fencing token 的独占租约，
// 并支持以当前 token 为条件的受保护写入。所有"校验 + 修改"均在同一个
// SQLite 事务内完成，不依赖进程内锁保证正确性。
type Service struct {
	db  *sql.DB
	now func() time.Time
}

// Option 用于定制 Service 行为。
type Option func(*Service)

// WithClock 注入时间源，主要用于测试租约到期行为。
func WithClock(now func() time.Time) Option {
	return func(s *Service) { s.now = now }
}

// Open 打开（必要时创建）位于 path 的 SQLite 数据库并返回租约服务。
// 传 ":memory:" 可获得纯内存实例。
func Open(path string, opts ...Option) (*Service, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_txlock=immediate", path)
	if path == ":memory:" {
		dsn = "file::memory:?_pragma=busy_timeout(5000)&_txlock=immediate"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// SQLite 单写者，限制连接数避免库内忙等。
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

func migrate(db *sql.DB) error {
	const schema = `
CREATE TABLE IF NOT EXISTS fencing_counters (
	resource   TEXT PRIMARY KEY,
	next_token INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS leases (
	resource   TEXT PRIMARY KEY,
	holder     TEXT NOT NULL,
	token      INTEGER NOT NULL,
	expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS idempotency_keys (
	request_id   TEXT PRIMARY KEY,
	request_hash TEXT NOT NULL,
	response     BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS resource_data (
	resource TEXT PRIMARY KEY,
	value    BLOB NOT NULL,
	token    INTEGER NOT NULL
);`
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("migrate schema: %w", err)
	}
	return nil
}

// Close 关闭底层数据库。
func (s *Service) Close() error { return s.db.Close() }

// Acquire 为 holder 申请 resource 上期限为 ttl 的独占租约。
// 每一次全新的成功获取都签发严格递增、永不复用的 fencing token。
// 租约仍被持有且未过期时返回 ErrBusy。
func (s *Service) Acquire(ctx context.Context, resource, holder string, ttl time.Duration) (Lease, error) {
	if err := validateLeaseArgs(resource, holder, ttl); err != nil {
		return Lease{}, err
	}
	now := s.now()
	expiresAt := now.Add(ttl)

	var lease Lease
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var curHolder string
		var curToken, curExpires int64
		switch err := tx.QueryRowContext(ctx,
			`SELECT holder, token, expires_at FROM leases WHERE resource = ?`, resource).
			Scan(&curHolder, &curToken, &curExpires); {
		case err == nil:
			if curExpires > now.UnixNano() {
				return fmt.Errorf("%w: resource %q held by %q until %s",
					ErrBusy, resource, curHolder, time.Unix(0, curExpires))
			}
		case errors.Is(err, sql.ErrNoRows):
		default:
			return err
		}

		var token int64
		if err := tx.QueryRowContext(ctx, `
INSERT INTO fencing_counters(resource, next_token) VALUES(?, 2)
ON CONFLICT(resource) DO UPDATE SET next_token = next_token + 1
RETURNING next_token - 1`, resource).Scan(&token); err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx, `
INSERT INTO leases(resource, holder, token, expires_at) VALUES(?,?,?,?)
ON CONFLICT(resource) DO UPDATE SET
	holder = excluded.holder,
	token = excluded.token,
	expires_at = excluded.expires_at`,
			resource, holder, token, expiresAt.UnixNano()); err != nil {
			return err
		}
		lease = Lease{Resource: resource, Holder: holder, Token: token, ExpiresAt: expiresAt}
		return nil
	})
	if err != nil {
		return Lease{}, err
	}
	return lease, nil
}

// Renew 由当前持有者续约谈 resource 上的租约，期限从当前时刻起算 ttl。
// 请求以 requestID 幂等：同一 requestID 重放相同内容返回首次的结果，
// 改换内容则返回 ErrIdempotencyConflict。
// token 不匹配返回 ErrStaleToken，租约已过期返回 ErrLeaseExpired。
func (s *Service) Renew(ctx context.Context, resource, holder string, token int64, requestID string, ttl time.Duration) (Lease, error) {
	if err := validateLeaseArgs(resource, holder, ttl); err != nil {
		return Lease{}, err
	}
	if err := validateTokenRequest(token, requestID); err != nil {
		return Lease{}, err
	}
	now := s.now()
	expiresAt := now.Add(ttl)
	hash := requestHash("renew", resource, holder, token, ttl)

	var lease Lease
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		resp, found, err := lookupIdempotent(ctx, tx, requestID, hash)
		if err != nil {
			return err
		}
		if found {
			lease = Lease{
				Resource:  resource,
				Holder:    holder,
				Token:     token,
				ExpiresAt: time.Unix(0, int64(binary.BigEndian.Uint64(resp))),
			}
			return nil
		}

		if err := checkHolder(ctx, tx, resource, holder, token, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE leases SET expires_at = ? WHERE resource = ?`,
			expiresAt.UnixNano(), resource); err != nil {
			return err
		}
		if err := recordIdempotent(ctx, tx, requestID, hash, encodeTime(expiresAt)); err != nil {
			return err
		}
		lease = Lease{Resource: resource, Holder: holder, Token: token, ExpiresAt: expiresAt}
		return nil
	})
	if err != nil {
		return Lease{}, err
	}
	return lease, nil
}

// Release 由当前持有者释放租约，以 requestID 幂等（语义同 Renew）。
// 释放后该 token 立即失效，之后的获取会签发更大的新 token。
func (s *Service) Release(ctx context.Context, resource, holder string, token int64, requestID string) error {
	if resource == "" || holder == "" {
		return fmt.Errorf("%w: resource and holder must be non-empty", ErrInvalidArgument)
	}
	if err := validateTokenRequest(token, requestID); err != nil {
		return err
	}
	now := s.now()
	hash := requestHash("release", resource, holder, token, 0)

	return s.withTx(ctx, func(tx *sql.Tx) error {
		_, found, err := lookupIdempotent(ctx, tx, requestID, hash)
		if err != nil {
			return err
		}
		if found {
			return nil
		}

		if err := checkHolder(ctx, tx, resource, holder, token, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM leases WHERE resource = ?`, resource); err != nil {
			return err
		}
		return recordIdempotent(ctx, tx, requestID, hash, []byte{})
	})
}

// Write 是受保护的条件写入：仅当 token 等于该资源当前有效租约的 token 时，
// 才把 value 持久化为资源数据。token 校验与数据写入处于同一个持久化事务，
// 过期或陈旧的 token 即使在进程内"看似持有锁"也无法写入。
// 失败的写入不会改变资源数据。
func (s *Service) Write(ctx context.Context, resource string, token int64, value []byte) error {
	if resource == "" {
		return fmt.Errorf("%w: resource must be non-empty", ErrInvalidArgument)
	}
	if token <= 0 {
		return fmt.Errorf("%w: token must be positive", ErrInvalidArgument)
	}
	now := s.now()

	return s.withTx(ctx, func(tx *sql.Tx) error {
		var curToken, curExpires int64
		switch err := tx.QueryRowContext(ctx,
			`SELECT token, expires_at FROM leases WHERE resource = ?`, resource).
			Scan(&curToken, &curExpires); {
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("%w: no active lease on %q", ErrLeaseExpired, resource)
		case err != nil:
			return err
		}
		if curToken != token {
			return fmt.Errorf("%w: current token is %d, got %d", ErrStaleToken, curToken, token)
		}
		if curExpires <= now.UnixNano() {
			return fmt.Errorf("%w: lease on %q expired", ErrLeaseExpired, resource)
		}
		_, err := tx.ExecContext(ctx, `
INSERT INTO resource_data(resource, value, token) VALUES(?,?,?)
ON CONFLICT(resource) DO UPDATE SET value = excluded.value, token = excluded.token`,
			resource, value, token)
		return err
	})
}

// Read 返回资源当前持久化的数据及写入它时使用的 token。
// 资源尚无数据时返回 (nil, 0, nil)。
func (s *Service) Read(ctx context.Context, resource string) ([]byte, int64, error) {
	if resource == "" {
		return nil, 0, fmt.Errorf("%w: resource must be non-empty", ErrInvalidArgument)
	}
	var value []byte
	var token int64
	switch err := s.db.QueryRowContext(ctx,
		`SELECT value, token FROM resource_data WHERE resource = ?`, resource).
		Scan(&value, &token); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, 0, nil
	case err != nil:
		return nil, 0, err
	}
	return value, token, nil
}

// Status 返回资源当前的租约状态与下一个将签发的 fencing token。
func (s *Service) Status(ctx context.Context, resource string) (Status, error) {
	if resource == "" {
		return Status{}, fmt.Errorf("%w: resource must be non-empty", ErrInvalidArgument)
	}
	st := Status{Resource: resource, NextToken: 1}
	now := s.now()

	err := s.withTx(ctx, func(tx *sql.Tx) error {
		switch err := tx.QueryRowContext(ctx,
			`SELECT next_token FROM fencing_counters WHERE resource = ?`, resource).
			Scan(&st.NextToken); {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return err
		}
		var expires int64
		switch err := tx.QueryRowContext(ctx,
			`SELECT holder, token, expires_at FROM leases WHERE resource = ?`, resource).
			Scan(&st.Holder, &st.Token, &expires); {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return err
		default:
			st.ExpiresAt = time.Unix(0, expires)
			st.Active = expires > now.UnixNano()
		}
		return nil
	})
	return st, err
}

// checkHolder 校验 resource 上存在由 holder 以 token 持有且未过期的租约。
func checkHolder(ctx context.Context, tx *sql.Tx, resource, holder string, token int64, now time.Time) error {
	var curHolder string
	var curToken, curExpires int64
	switch err := tx.QueryRowContext(ctx,
		`SELECT holder, token, expires_at FROM leases WHERE resource = ?`, resource).
		Scan(&curHolder, &curToken, &curExpires); {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("%w: no active lease on %q", ErrLeaseExpired, resource)
	case err != nil:
		return err
	}
	if curToken != token || curHolder != holder {
		return fmt.Errorf("%w: lease on %q held by %q with token %d",
			ErrStaleToken, resource, curHolder, curToken)
	}
	if curExpires <= now.UnixNano() {
		return fmt.Errorf("%w: lease on %q expired", ErrLeaseExpired, resource)
	}
	return nil
}

func (s *Service) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// lookupIdempotent 查询 requestID 的重放记录。内容不一致时返回 ErrIdempotencyConflict。
func lookupIdempotent(ctx context.Context, tx *sql.Tx, requestID, hash string) (resp []byte, found bool, err error) {
	var storedHash string
	switch err := tx.QueryRowContext(ctx,
		`SELECT request_hash, response FROM idempotency_keys WHERE request_id = ?`, requestID).
		Scan(&storedHash, &resp); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, err
	}
	if storedHash != hash {
		return nil, false, fmt.Errorf("%w: request %q replayed with different payload",
			ErrIdempotencyConflict, requestID)
	}
	return resp, true, nil
}

func recordIdempotent(ctx context.Context, tx *sql.Tx, requestID, hash string, resp []byte) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO idempotency_keys(request_id, request_hash, response) VALUES(?,?,?)`,
		requestID, hash, resp)
	return err
}

func requestHash(op, resource, holder string, token int64, ttl time.Duration) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%d\x00%d", op, resource, holder, token, ttl)
	return hex.EncodeToString(h.Sum(nil))
}

func encodeTime(t time.Time) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(t.UnixNano()))
	return b[:]
}

func validateLeaseArgs(resource, holder string, ttl time.Duration) error {
	if resource == "" || holder == "" {
		return fmt.Errorf("%w: resource and holder must be non-empty", ErrInvalidArgument)
	}
	if ttl <= 0 {
		return fmt.Errorf("%w: ttl must be positive", ErrInvalidArgument)
	}
	return nil
}

func validateTokenRequest(token int64, requestID string) error {
	if token <= 0 {
		return fmt.Errorf("%w: token must be positive", ErrInvalidArgument)
	}
	if requestID == "" {
		return fmt.Errorf("%w: request id must be non-empty", ErrInvalidArgument)
	}
	return nil
}
