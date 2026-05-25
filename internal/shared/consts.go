package shared

import "time"

const (
	DefaultSnapshotInterval = 1500 * time.Millisecond
	MaxLimit 				= 2000
	DefaultLimit			= 5
	DefaultHalfLifeMin      = 2.5
	GracefulShutdownTimeout = 10 * time.Second
	ReadTimeout             = 5 * time.Second
	WriteTimeout            = 5 * time.Second
	StreamName				= "search_trends"
	ConsumerName			= "search_trends"
	IdempotencyTTL			= 5 * time.Minute
	AckWait					= 30 * time.Second
	IdleTimeout				= 2 * time.Minute
	EventMaxAge				= 10 * time.Minute
)