package consistenthash

import (
	"hash/crc32"
	"sort"
	"sync"
)

// consistent hash provides a basic utility for managing consistent hash rings

type ConsistentHash struct {
	mu       sync.RWMutex
	nodes    map[uint32]string // Hash of the node -> Node ID
	hashRing []uint32          // Sorted list of hashes on the ring
}

// New creates a new ConsistentHash instance with the given nodes.
func New(nodeIDs []string) *ConsistentHash {
	h := &ConsistentHash{
		nodes:    make(map[uint32]string),
		hashRing: []uint32{},
	}

	for _, nodeID := range nodeIDs {
		h.addNode(nodeID)
	}

	return h
}

// AddNode adds a new node to the consistent hash ring.
func (h *ConsistentHash) AddNode(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.addNode(nodeID)
}

func (h *ConsistentHash) addNode(nodeID string) {
	hash := h.hashKey(nodeID)
	if _, exists := h.nodes[hash]; exists {
		return // Node already exists
	}

	h.nodes[hash] = nodeID
	h.hashRing = append(h.hashRing, hash)
	sort.Slice(h.hashRing, func(i, j int) bool { return h.hashRing[i] < h.hashRing[j] })
}

// RemoveNode removes a node from the consistent hash ring.
func (h *ConsistentHash) RemoveNode(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	hash := h.hashKey(nodeID)
	if _, exists := h.nodes[hash]; !exists {
		return // Node does not exist
	}

	delete(h.nodes, hash)
	for i, v := range h.hashRing {
		if v == hash {
			h.hashRing = append(h.hashRing[:i], h.hashRing[i+1:]...)
			break
		}
	}
}

// GetNode returns the ID of the appropriate node for a given key (e.g., customer ID).
func (h *ConsistentHash) GetNode(key string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.hashRing) == 0 {
		return "" // No nodes available
	}

	hash := h.hashKey(key)

	// Find the first node clockwise from the hash.
	idx := sort.Search(len(h.hashRing), func(i int) bool { return h.hashRing[i] >= hash })
	if idx == len(h.hashRing) {
		idx = 0 // Wrap around to the first node
	}

	return h.nodes[h.hashRing[idx]]
}

// hashKey generates a consistent hash for a given key.
func (h *ConsistentHash) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
