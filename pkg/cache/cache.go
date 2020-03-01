package cache

import (
	"context"
	"github.com/rueian/groupcache/pkg/data"
	"github.com/rueian/groupcache/pkg/flight"
	"github.com/rueian/groupcache/pkg/peer"
	"math/rand"
	"strconv"
	"sync/atomic"
)

type Group struct {
	name     string
	maxBytes int64
	flights  *flight.Group
	peers    peer.Picker
	loader   data.Loader

	main data.Store
	hot  data.Store

	delegate int
	replicas int

	_ int32 // force Stats to be 8-byte aligned on 32-bit platforms

	// Stats are statistics on the group.
	Stats Stats
}

func (g *Group) Get(ctx context.Context, key string) (value data.Value, err error) {
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
			// if the local cache is not acceptable and final data is not loaded
			// then reset the local cache state by calling Questionable()
			if value == nil {
				d.Questionable()
			}
		}()
	}

	value, err = g.flights.Do(key, func() (value data.Value, err error) {
		// try again in local cache, it could be just filled by another flight
		if value = g.LocalLookup(key); value != nil {
			atomic.AddInt64(&g.Stats.CacheHits, 1)
			return
		}

		for i, replica := range replicas {
			peers := g.peers.Pick(replica, g.delegate)
			if len(peers) > 0 {
				if !peers[0].Self() {
					// load from remote mockpeer, using replica to prevent mockpeer failure
					if value, err = peers[0].LookupOrLoad(ctx, key); err != nil {
						if i == 0 {
							atomic.AddInt64(&g.Stats.PeerErrors, 1)
						} else {
							atomic.AddInt64(&g.Stats.ReplicaErrors, 1)
						}
						continue
					} else if rand.Intn(10) == 0 {
						g.populateCache(key, value, g.hot)
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
						if value, err = peer.Lookup(ctx, key); value != nil {
							atomic.AddInt64(&g.Stats.NeighborWarmUpLoads, 1)
							break
						} else {
							atomic.AddInt64(&g.Stats.NeighborWarmUpErrors, 1)
						}
					}
				}
				break
			}
		}
		if value == nil {
			value, err = g.loader.Load(ctx, key)
			if err != nil {
				atomic.AddInt64(&g.Stats.LocalLoads, 1)
			} else {
				atomic.AddInt64(&g.Stats.LocalLoadErrs, 1)
			}
		}
		if value != nil {
			g.populateCache(key, value, g.main)
			err = nil
			return
		}
		return
	})
	atomic.AddInt64(&g.Stats.LoadsDeduped, 1)
	if value != nil {
		// successfully load data, push to other replicas
		for _, replica := range replicas {
			peers := g.peers.Pick(replica, 1)
			if len(peers) > 0 && !peers[0].Self() {
				_ = peers[0].Push(ctx, key, value)
			}
		}
	}
	return
}

func (g *Group) populateCache(key string, data data.Value, store data.Store) {
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

func (g *Group) LocalLookup(key string) data.Value {
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
	LoadsDeduped   int64 // after flight
	LocalLoads     int64 // total good local loads
	LocalLoadErrs  int64 // total bad local loads
	ServerRequests int64 // gets that came over the network from peers

	ReplicaLoads         int64 // remote load or remote cache hit from replicas
	ReplicaErrors        int64
	NeighborWarmUpLoads  int64 // hit from neighbor during warm up
	NeighborWarmUpErrors int64 // error from neighbor during warm up
}
