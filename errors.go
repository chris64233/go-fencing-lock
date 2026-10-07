package fencinglock

import "errors"

var (
	// ErrInvalidRequest is returned when a request is missing required fields
	// or carries illegal values.
	ErrInvalidRequest = errors.New("fencinglock: invalid request")

	// ErrUnavailable is returned when at least one resource cannot be leased.
	ErrUnavailable = errors.New("fencinglock: resource unavailable")

	// ErrLeaseExpired is returned when the lease backing an operation has expired.
	ErrLeaseExpired = errors.New("fencinglock: lease expired")

	// ErrLeaseReleased is returned when the lease backing an operation was released.
	ErrLeaseReleased = errors.New("fencinglock: lease released")

	// ErrFencingToken is returned when a stale or unknown fencing token is used.
	ErrFencingToken = errors.New("fencinglock: stale fencing token")

	// ErrHolderMismatch is returned when the caller does not own the lease.
	ErrHolderMismatch = errors.New("fencinglock: holder mismatch")

	// ErrRequestConflict is returned when an idempotency key is reused with
	// different request content.
	ErrRequestConflict = errors.New("fencinglock: idempotency key conflict")

	// ErrCompositeInvalid is returned when a composite lease is broken because
	// a member was taken over, expired, released or otherwise diverged.
	ErrCompositeInvalid = errors.New("fencinglock: composite lease invalid")

	// ErrCompositeNotFound is returned for an unknown composite lease id.
	ErrCompositeNotFound = errors.New("fencinglock: composite lease not found")
)
