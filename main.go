package main

import (
	"fmt"
	"sync"
	"time"
)

// IPIdentity represents the security identity associated with an IP.
type IPIdentity struct {
	Identity  int
	Timestamp time.Time
}

// IPCache manages the IP-to-Identity mappings with concurrency control and versioning.
type IPCache struct {
	mu   sync.RWMutex
	maps map[string]IPIdentity
}

func NewIPCache() *IPCache {
	return &IPCache{
		maps: make(map[string]IPIdentity),
	}
}

// Upsert updates the identity for an IP if the update is newer than the existing one.
func (c *IPCache) Upsert(ip string, identity int, ts time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	existing, exists := c.maps[ip]
	if exists && ts.Before(existing.Timestamp) {
		// Reject stale update
		return false
	}

	c.maps[ip] = IPIdentity{
		Identity:  identity,
		Timestamp: ts,
	}
	return true
}

// Delete removes the identity for an IP if the delete event is newer than the existing mapping.
func (c *IPCache) Delete(ip string, ts time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	existing, exists := c.maps[ip]
	if !exists {
		return false
	}

	if ts.Before(existing.Timestamp) {
		// Reject stale delete
		return false
	}

	delete(c.maps, ip)
	return true
}

// Get retrieves the identity for an IP.
func (c *IPCache) Get(ip string) (int, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	id, exists := c.maps[ip]
	if !exists {
		return 0, false
	}
	return id.Identity, true
}

func main() {
	cache := NewIPCache()
	ip := "10.244.0.5"

	// Simulate rapid events
	t1 := time.Now()
	t2 := t1.Add(10 * time.Millisecond)
	t3 := t2.Add(10 * time.Millisecond)

	// 1. Spawn endpoint with IP: 10.244.0.5 and Identity: 1001
	cache.Upsert(ip, 1001, t1)

	// 2. Trigger delete event for IP: 10.244.0.5 (simulated delay/out-of-order execution)
	// 3. Immediately trigger add event for IP: 10.244.0.5 with Identity: 1002
	cache.Upsert(ip, 1002, t3)

	// Late delete event arrives (timestamp t2, which is older than t3)
	cache.Delete(ip, t2)

	// Assert final state
	identity, exists := cache.Get(ip)
	if exists && identity == 1002 {
		fmt.Printf("Success: IP %s has correct Identity %d\n", ip, identity)
	} else {
		fmt.Printf("Failure: IP %s has Identity %d (exists: %v)\n", ip, identity, exists)
	}
}
