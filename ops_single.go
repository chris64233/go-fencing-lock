package fencinglock

// Acquire 申请单个资源的租约。
func (s *Service) Acquire(req AcquireRequest) (*LeaseResult, error) {
	if req.ResourceID == "" || req.Holder == "" || req.TTL <= 0 {
		return nil, ErrInvalidRequest
	}
	fp := "acquire:" + req.ResourceID + "|" + req.Holder + "|" + ttlFingerprint(req.TTL)

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.scanExpiredLocked(now)

	if req.RequestID != "" {
		if resp, err, ok := s.replay(req.RequestID, opAcquire, fp); ok {
			if err != nil {
				return nil, err
			}
			return cloneLease(resp.(*LeaseResult)), nil
		}
	}

	r := s.getOrCreate(req.ResourceID)
	if r.holder != "" && r.holder != req.Holder {
		err := &UnavailableError{Resource: req.ResourceID, Reason: ReasonHeld}
		s.remember(req.RequestID, opAcquire, fp, nil, err)
		return nil, err
	}

	r.token = s.nextToken()
	r.holder = req.Holder
	r.groupID = 0
	r.expiresAt = now.Add(req.TTL)
	r.lastReason = ReasonUnknown

	res := &LeaseResult{
		ResourceID: req.ResourceID,
		Holder:     req.Holder,
		Token:      r.token,
		ExpiresAt:  r.expiresAt,
	}
	s.remember(req.RequestID, opAcquire, fp, res, nil)
	return cloneLease(res), nil
}

// Renew 续租单个资源；该资源当前不能属于有效组合租约。
func (s *Service) Renew(req RenewRequest) (*LeaseResult, error) {
	if req.ResourceID == "" || req.Holder == "" || req.TTL <= 0 {
		return nil, ErrInvalidRequest
	}
	fp := "renew:" + req.ResourceID + "|" + req.Holder + "|" + ttlFingerprint(req.TTL)

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.scanExpiredLocked(now)

	if req.RequestID != "" {
		if resp, err, ok := s.replay(req.RequestID, opRenew, fp); ok {
			if err != nil {
				return nil, err
			}
			return cloneLease(resp.(*LeaseResult)), nil
		}
	}

	r, ok := s.resources[req.ResourceID]
	var err error
	switch {
	case !ok || r.holder == "":
		err = ErrNotHolder
	case r.holder != req.Holder:
		err = ErrNotHolder
	case r.groupID != 0:
		err = ErrSplitGroup
	}
	if err != nil {
		s.remember(req.RequestID, opRenew, fp, nil, err)
		return nil, err
	}

	r.expiresAt = now.Add(req.TTL)
	res := &LeaseResult{
		ResourceID: req.ResourceID,
		Holder:     r.holder,
		Token:      r.token,
		ExpiresAt:  r.expiresAt,
	}
	s.remember(req.RequestID, opRenew, fp, res, nil)
	return cloneLease(res), nil
}

// Release 释放单个资源；该资源当前不能属于有效组合租约。
func (s *Service) Release(req ReleaseRequest) error {
	if req.ResourceID == "" || req.Holder == "" {
		return ErrInvalidRequest
	}
	fp := "release:" + req.ResourceID + "|" + req.Holder

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.scanExpiredLocked(now)

	if req.RequestID != "" {
		if _, err, ok := s.replay(req.RequestID, opRelease, fp); ok {
			return err
		}
	}

	r, ok := s.resources[req.ResourceID]
	var err error
	switch {
	case !ok || r.holder == "":
		err = ErrNotHolder
	case r.holder != req.Holder:
		err = ErrNotHolder
	case r.groupID != 0:
		err = ErrSplitGroup
	}
	if err != nil {
		s.remember(req.RequestID, opRelease, fp, nil, err)
		return err
	}

	s.clearLeaseLocked(r, ReasonReleased)
	s.remember(req.RequestID, opRelease, fp, nil, nil)
	return nil
}

// Transfer 交接单个资源的持有关系：校验 From 为当前持有者，
// 为 To 颁发新的 fencing token。若资源属于某个组合租约，
// 该组合版本立即整体失效（其余资源不能再以旧组合身份写入）。
func (s *Service) Transfer(req TransferRequest) (*LeaseResult, error) {
	if req.ResourceID == "" || req.From == "" || req.To == "" || req.TTL <= 0 {
		return nil, ErrInvalidRequest
	}
	fp := "transfer:" + req.ResourceID + "|" + req.From + "|" + req.To + "|" + ttlFingerprint(req.TTL)

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.scanExpiredLocked(now)

	if req.RequestID != "" {
		if resp, err, ok := s.replay(req.RequestID, opTransfer, fp); ok {
			if err != nil {
				return nil, err
			}
			return cloneLease(resp.(*LeaseResult)), nil
		}
	}

	r, ok := s.resources[req.ResourceID]
	var err error
	switch {
	case !ok || r.holder == "":
		err = ErrNotHolder
	case r.holder != req.From:
		err = ErrNotHolder
	}
	if err != nil {
		s.remember(req.RequestID, opTransfer, fp, nil, err)
		return nil, err
	}

	if r.groupID != 0 {
		s.breakGroupLocked(r.groupID, ReasonTakenOver)
	}

	r.token = s.nextToken()
	r.holder = req.To
	r.groupID = 0
	r.expiresAt = now.Add(req.TTL)
	r.lastReason = ReasonUnknown

	res := &LeaseResult{
		ResourceID: req.ResourceID,
		Holder:     req.To,
		Token:      r.token,
		ExpiresAt:  r.expiresAt,
	}
	s.remember(req.RequestID, opTransfer, fp, res, nil)
	return cloneLease(res), nil
}
