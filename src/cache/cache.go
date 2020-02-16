package cache

import (
	"context"
	"math/rand"
	"strconv"
	"sync/atomic"
)

type PeerPicker interface {
	// should return peers in sequence of trying
	Pick(key string, max int) []Peer
}

type Peer interface {
	Lookup(ctx context.Context, key string) (Data, error)
	LookupOrLoad(ctx context.Context, key string) (Data, error)
	Push(ctx context.Context, key string, data Data) error
	Self() bool
	WarmingUp() bool
}

type DataLoader interface {
	Load(ctx context.Context, key string) (Data, error)
}

type FlightGroup interface {
	// Done is called when Do is done.
	Do(key string, fn func() (Data, error)) (Data, error)
}

type Store interface {
	Get(key string) Data
	Add(key string, data Data)
	RemoveOne()
	Bytes() int64
}

type Data interface {
	Val() []byte
	Acceptable() bool
	Questionable()
}

type Group struct {
	name     string
	maxBytes int64
	flights  FlightGroup
	peers    PeerPicker
	loader   DataLoader

	main Store
	hot  Store

	delegate int
	replicas int

	_ int32 // force Stats to be 8-byte aligned on 32-bit platforms

	// Stats are statistics on the group.
	Stats Stats
}

func (g *Group) Get(ctx context.Context, key string) (data Data, err error) {
	atomic.AddInt64(&g.Stats.Gets, 1)

	replicas := make([]string, g.replicas)
	for i := 0; i < g.replicas; i++ {
		replicas[i] = key + strconv.Itoa(i)
	}

	if d := g.LocalLookup(key); d != nil {
		if d.Acceptable() {
			atomic.AddInt64(&g.Stats.CacheHits, 1)
			return d, nil
		}
		defer func() {
			if data == nil {
				d.Questionable()
			}
		}()
	}

	data, err = g.flights.Do(key, func() (data Data, err error) {
		// try again in local cache, it could be just filled by another flight
		if data = g.LocalLookup(key); data != nil {
			atomic.AddInt64(&g.Stats.CacheHits, 1)
			return
		}

		for i, replica := range replicas {
			peers := g.peers.Pick(replica, g.delegate)
			if len(peers) > 0 {
				if !peers[0].Self() {
					// load from remote peer, using replica to prevent peer failure
					if data, err = peers[0].LookupOrLoad(ctx, key); err != nil {
						if i == 0 {
							atomic.AddInt64(&g.Stats.PeerErrors, 1)
						} else {
							atomic.AddInt64(&g.Stats.ReplicaErrors, 1)
						}
						continue
					} else if rand.Intn(10) == 0 {
						g.populateCache(replica, data, g.hot)
					}

					if i == 0 {
						atomic.AddInt64(&g.Stats.PeerLoads, 1)
					} else {
						atomic.AddInt64(&g.Stats.ReplicaLoads, 1)
					}
					err = nil
					return
				}
				// try neighbors during warming up
				if peers[0].WarmingUp() {
					for _, peer := range peers[1:] {
						if data, err = peer.Lookup(ctx, key); data != nil {
							atomic.AddInt64(&g.Stats.NeighborWarmUpLoads, 1)
							break
						} else {
							atomic.AddInt64(&g.Stats.NeighborWarmUpErrors, 1)
						}
					}
				}
			}
			if data == nil {
				data, err = g.loader.Load(ctx, key)
				if err != nil {
					atomic.AddInt64(&g.Stats.LocalLoads, 1)
				} else {
					atomic.AddInt64(&g.Stats.LocalLoadErrs, 1)
				}
			}
			if data != nil {
				g.populateCache(replica, data, g.main)
				err = nil
				return
			}
		}
		return
	})
	atomic.AddInt64(&g.Stats.LoadsDeduped, 1)
	if data != nil {
		// successfully load data, push to other replicas
		for _, replica := range replicas {
			peers := g.peers.Pick(replica, 1)
			if len(peers) > 0 && !peers[0].Self() {
				_ = peers[0].Push(ctx, key, data)
			}
		}
	}
	return
}

func (g *Group) populateCache(key string, data Data, store Store) {
	if g.maxBytes <= 0 {
		return
	}
	store.Add(key, data)

	// Evict items from if necessary.
	for {
		mainBytes := g.main.Bytes()
		hotBytes := g.hot.Bytes()
		if mainBytes+hotBytes <= g.maxBytes {
			return
		}

		// TODO(bradfitz): this is good-enough-for-now logic.
		// It should be something based on measurements and/or
		// respecting the costs of different resources.
		victim := g.main
		if hotBytes > mainBytes/8 {
			victim = g.hot
		}
		victim.RemoveOne()
	}
}

func (g *Group) LocalLookup(key string) Data {
	if v := g.hot.Get(key); v != nil {
		return v
	}
	return g.main.Get(key)
}

type Stats struct {
	Gets           int64 // any Get request, including from peers
	CacheHits      int64 // either cache was good
	PeerLoads      int64 // either remote load or remote cache hit (not an error)
	PeerErrors     int64
	LoadsDeduped   int64 // after singleflight
	LocalLoads     int64 // total good local loads
	LocalLoadErrs  int64 // total bad local loads
	ServerRequests int64 // gets that came over the network from peers

	ReplicaLoads         int64 // remote load or remote cache hit from replicas
	ReplicaErrors        int64
	NeighborWarmUpLoads  int64 // hit from neighbor during warm up
	NeighborWarmUpErrors int64 // error from neighbor during warm up
}
