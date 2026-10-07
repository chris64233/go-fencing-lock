package fencinglock

import "time"

// CompositeHandoffRequest atomically transfers a whole composite lease to a
// new holder. Every member receives a new independent fencing token and the
// composite version advances, so the old holder's tokens can never write again.
type CompositeHandoffRequest struct {
	CompositeID string
	From        string
	To          string
	TTL         time.Duration
	RequestID   string
}

// CompositeHandoff hands the entire group over in one version boundary.
func (s *Service) CompositeHandoff(req CompositeHandoffRequest) (*CompositeLease, error) {
	if req.CompositeID == "" || req.From == "" || req.To == "" || req.TTL <= 0 {
		return nil, ErrInvalidRequest
	}
	content := requestContent{
		resources: []string{req.CompositeID},
		holder:    req.To,
		ttl:       req.TTL,
		from:      req.From,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &idemRecord{kind: "composite-handoff", content: content}
	rec, replayed, err := s.replayOrBegin(rec, "composite-handoff", req.RequestID, content)
	if err != nil {
		return nil, err
	}
	if replayed {
		return rec.compositeOut, rec.err
	}

	now := s.now()
	c, ok := s.composites[req.CompositeID]
	if !ok {
		rec.err = ErrCompositeNotFound
		return nil, rec.err
	}
	if c.released {
		rec.err = ErrLeaseReleased
		return nil, rec.err
	}
	if c.holder != req.From {
		rec.err = ErrHolderMismatch
		return nil, rec.err
	}
	if err := groupIntact(s, c, now); err != nil {
		rec.err = err
		return nil, err
	}

	c.holder = req.To
	c.version++
	c.expires = now.Add(req.TTL)
	c.ttl = req.TTL
	for _, name := range c.members {
		l := s.resources[name]
		l.fencingToken++
		l.holder = req.To
		l.expiresAt = c.expires
		c.tokens[name] = l.fencingToken
	}

	out := compositeView(c)
	rec.compositeOut = out
	return out, nil
}
