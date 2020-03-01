package data

import (
	"context"
	"sync/atomic"
	"time"
)

type Loader interface {
	Load(ctx context.Context, key string) (Value, error)
}

type Value interface {
	Val() []byte
	Acceptable() bool
	Questionable()
}

type Store interface {
	Get(key string) Value
	Add(key string, data Value)
	RemoveOne()
	Bytes() int64
}

type TTLValue struct {
	val []byte
	ttl time.Duration
	ts  time.Time
	hit int64
}

func (t *TTLValue) Val() []byte {
	return t.val
}

func (t *TTLValue) Acceptable() bool {
	if time.Since(t.ts) <= t.ttl {
		return true
	}
	return atomic.AddInt64(&t.hit, 1) > 1
}

func (t *TTLValue) Questionable() {
	atomic.StoreInt64(&t.hit, 0)
}
