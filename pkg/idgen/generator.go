package idgen

import (
	"fmt"
	"sync"

	"github.com/bwmarrin/snowflake"
)

// Generator defines the interface for generating unique IDs
type Generator interface {
	GenerateID() int64
}

// SnowflakeGenerator implements the Generator interface using Twitter Snowflake
type SnowflakeGenerator struct {
	node *snowflake.Node
	mu   sync.Mutex
}

// NewSnowflakeGenerator initializes a new ID generator.
// nodeID must be unique per server instance (0-1023) to prevent collisions.
func NewSnowflakeGenerator(nodeID int64) (*SnowflakeGenerator, error) {
	node, err := snowflake.NewNode(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to create snowflake node: %w", err)
	}

	return &SnowflakeGenerator{
		node: node,
	}, nil
}

// GenerateID returns a new unique 64-bit integer ID
func (g *SnowflakeGenerator) GenerateID() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Int64() returns the ID as an int64
	return g.node.Generate().Int64()
}
