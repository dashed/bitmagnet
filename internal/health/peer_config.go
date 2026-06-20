package health

import "time"

// PeerConfig lets an HTTP-only serving role expose a fleet-level health view by
// aggregating status from one or more peer GraphQL endpoints.
type PeerConfig struct {
	PeerGraphqlURLs []string
	PeerTimeout     time.Duration
}

func NewDefaultPeerConfig() PeerConfig {
	return PeerConfig{
		PeerTimeout: 1500 * time.Millisecond,
	}
}
