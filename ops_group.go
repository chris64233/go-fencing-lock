package fencinglock

import (
	"strconv"
	"strings"
)

func intToString(v int64) string { return strconv.FormatInt(v, 10) }

// AcquireGroup 原子地申请整组资源。
//
// 资源编号先去重再按字典序处理；校验阶段不修改任何状态，
// 任一资源被其他有效持有者占用时直接失败，不留下半组状态，
// 原先仍有效的单资源租约（含同持有者的）保持不变。
func (s *Service) AcquireGroup(req GroupRequest) (*GroupResult, error) {
	ids := normalizeIDs(req.ResourceIDs)
	if req.Holder == "" || req.TTL <= 0 || len(ids) == 0 {
		return nil, ErrInvalidRequest
	}
	fp := "acquiregroup:" + strings.Join(ids, ",") + "|" + req.Holder + "|" + ttlFingerprint(req.TTL)

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.scanExpiredLocked(now)

	if req.RequestID != "" {
		if resp, err, ok := s.replay(req.RequestID, opAcquireGroup, fp); ok {
			if err != nil {
				return nil, err
			}
			return cloneGroup(resp.(*GroupResult)), nil
		}
	}

	// 第一阶段：只读校验。不修改任何资源，失败即零回滚成本，
	// 不会出现半组已取得的可见状态。
	for _, id := range ids {
		r := s.getOrCreate(id)
		if r.holder != "" && !r.expired(now) && r.holder != req.Holder {
			err := &UnavailableError{Resource: id, Reason: ReasonHeld}
			s.remember(req.RequestID, opAcquireGroup, fp, nil, err)
			return nil, err
		}
	}

	// 第二阶段：同一版本边界内整体提交，所有成员拿到同一 GroupID。
	gid := s.nextGroupID()
	expiresAt := now.Add(req.TTL)
	tokens := make(map[string]int64, len(ids))
	for _, id := range ids {
		r := s.getOrCreate(id)
		// 过期资源在此刻才回收；校验失败不会执行到这里，
		// 因此原先仍有效的单资源租约不会被误删。
		if r.holder != "" && r.expired(now) {
			if r.groupID != 0 {
				s.breakGroupLocked(r.groupID, ReasonExpired)
			} else {
				s.clearLeaseLocked(r, ReasonExpired)
			}
		}
		r.token = s.nextToken()
		r.holder = req.Holder
		r.groupID = gid
		r.expiresAt = expiresAt
		r.lastReason = ReasonUnknown
		tokens[id] = r.token
	}
	s.groups[gid] = &groupMeta{
		id:        gid,
		resources: append([]string(nil), ids...),
		holder:    req.Holder,
		expiresAt: expiresAt,
		active:    true,
	}

	res := &GroupResult{
		GroupID:   gid,
		Holder:    req.Holder,
		Resources: append([]string(nil), ids...),
		Tokens:    tokens,
		ExpiresAt: expiresAt,
	}
	s.remember(req.RequestID, opAcquireGroup, fp, res, nil)
	return cloneGroup(res), nil
}

// checkActiveGroup 校验组合版本存在、有效、持有者匹配且整组未过期。
// 返回组元数据；任一条件不满足返回对应错误。
func (s *Service) checkActiveGroup(gid int64, holder string) (*groupMeta, error) {
	g, ok := s.groups[gid]
	if !ok {
		return nil, ErrInvalidGroup
	}
	if !g.active {
		return nil, ErrInvalidGroup
	}
	now := s.now()
	if !now.Before(g.expiresAt) {
		s.breakGroupLocked(gid, ReasonExpired)
		return nil, ErrInvalidGroup
	}
	if g.holder != holder {
		return nil, ErrNotHolder
	}
	// 防御性校验：所有成员都必须仍指向该组。
	for _, id := range g.resources {
		r := s.resources[id]
		if r == nil || r.groupID != gid {
			s.breakGroupLocked(gid, ReasonTakenOver)
			return nil, ErrInvalidGroup
		}
	}
	return g, nil
}

// RenewGroup 按组合版本续租整组；旧版本（被接管/过期/释放）一律拒绝。
func (s *Service) RenewGroup(req GroupRenewRequest) (*GroupResult, error) {
	if req.GroupID <= 0 || req.Holder == "" || req.TTL <= 0 {
		return nil, ErrInvalidRequest
	}
	fp := "renewgroup:" + intToString(req.GroupID) + "|" + req.Holder + "|" + ttlFingerprint(req.TTL)

	s.mu.Lock()
	defer s.mu.Unlock()

	if req.RequestID != "" {
		if resp, err, ok := s.replay(req.RequestID, opRenewGroup, fp); ok {
			if err != nil {
				return nil, err
			}
			return cloneGroup(resp.(*GroupResult)), nil
		}
	}

	g, err := s.checkActiveGroup(req.GroupID, req.Holder)
	if err != nil {
		s.remember(req.RequestID, opRenewGroup, fp, nil, err)
		return nil, err
	}

	expiresAt := s.now().Add(req.TTL)
	g.expiresAt = expiresAt
	for _, id := range g.resources {
		s.resources[id].expiresAt = expiresAt
	}

	tokens := make(map[string]int64, len(g.resources))
	for _, id := range g.resources {
		tokens[id] = s.resources[id].token
	}
	res := &GroupResult{
		GroupID:   g.id,
		Holder:    g.holder,
		Resources: append([]string(nil), g.resources...),
		Tokens:    tokens,
		ExpiresAt: expiresAt,
	}
	s.remember(req.RequestID, opRenewGroup, fp, res, nil)
	return cloneGroup(res), nil
}

// ReleaseGroup 按组合版本释放整组；成员一起释放，不存在
// “一个先释放、另一个仍被旧组合认为有效”的窗口。
func (s *Service) ReleaseGroup(req GroupOpRequest) error {
	if req.GroupID <= 0 || req.Holder == "" {
		return ErrInvalidRequest
	}
	fp := "releasegroup:" + intToString(req.GroupID) + "|" + req.Holder

	s.mu.Lock()
	defer s.mu.Unlock()

	if req.RequestID != "" {
		if _, err, ok := s.replay(req.RequestID, opReleaseGroup, fp); ok {
			return err
		}
	}

	g, err := s.checkActiveGroup(req.GroupID, req.Holder)
	if err != nil {
		s.remember(req.RequestID, opReleaseGroup, fp, nil, err)
		return err
	}

	for _, id := range g.resources {
		s.clearLeaseLocked(s.resources[id], ReasonReleased)
	}
	g.active = false
	g.invalidReason = ReasonReleased

	s.remember(req.RequestID, opReleaseGroup, fp, nil, nil)
	return nil
}

// WriteGroup 用组合租约执行受保护写入前的校验。
// 要求：版本有效、持有者匹配、token 集合与组内成员一一相等。
// 任一组员已被接管或过期时，旧组合对其它资源的写入同样被拒绝。
func (s *Service) WriteGroup(req GroupWriteRequest) error {
	if req.GroupID <= 0 || req.Holder == "" {
		return ErrInvalidRequest
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	g, err := s.checkActiveGroup(req.GroupID, req.Holder)
	if err != nil {
		return err
	}
	if len(req.Tokens) != len(g.resources) {
		return ErrInvalidSet
	}
	for _, id := range g.resources {
		tok, ok := req.Tokens[id]
		if !ok {
			return ErrInvalidSet
		}
		if tok != s.resources[id].token {
			return ErrInvalidToken
		}
	}
	return nil
}

// Write 用单资源租约执行受保护写入前的校验。
// 资源属于有效组合时必须走 WriteGroup。
func (s *Service) Write(req WriteRequest) error {
	if req.ResourceID == "" || req.Holder == "" {
		return ErrInvalidRequest
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.scanExpiredLocked(now)

	r, ok := s.resources[req.ResourceID]
	if !ok || r.holder == "" {
		return ErrNotFound
	}
	if r.groupID != 0 {
		return ErrSplitGroup
	}
	if r.holder != req.Holder {
		return ErrNotHolder
	}
	if req.Token != r.token {
		return ErrInvalidToken
	}
	return nil
}
