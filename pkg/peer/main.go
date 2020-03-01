package peer

import (
	"context"
	"github.com/rueian/groupcache/pkg/data"
)

type Picker interface {
	// should return peers in sequence of trying
	Pick(key string, max int) []Peer
}

type Peer interface {
	Lookup(ctx context.Context, key string) (data.Value, error)
	LookupOrLoad(ctx context.Context, key string) (data.Value, error)
	Push(ctx context.Context, key string, data data.Value) error
	Self() bool
	WarmingUp() bool
}
