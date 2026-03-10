package raft

import (
	"fmt"
	"net"
	"time"

	hraft "github.com/hashicorp/raft"
)

// TransportConfig holds configuration for the RAFT transport layer.
type TransportConfig struct {
	// BindAddr is the address to bind the RAFT transport to (e.g., "0.0.0.0:7400").
	BindAddr string
	// AdvertiseAddr is the address advertised to peers (e.g., "naladb-0:7400").
	// If empty, BindAddr is used.
	AdvertiseAddr string
	// MaxPool is the maximum number of connections to maintain in the pool.
	MaxPool int
	// Timeout is the connection timeout for transport operations.
	Timeout time.Duration
}

// DefaultTransportConfig returns a TransportConfig with sensible defaults.
func DefaultTransportConfig(bindAddr string) TransportConfig {
	return TransportConfig{
		BindAddr: bindAddr,
		MaxPool:  5,
		Timeout:  10 * time.Second,
	}
}

// NewTCPTransport creates a TCP-based RAFT transport. This is the standard
// transport for production deployments, using hashicorp/raft's built-in
// TCP stream layer.
func NewTCPTransport(cfg TransportConfig) (*hraft.NetworkTransport, error) {
	bindAddr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("raft: resolve bind addr: %w", err)
	}

	var advertise net.Addr
	if cfg.AdvertiseAddr != "" {
		advertise, err = net.ResolveTCPAddr("tcp", cfg.AdvertiseAddr)
		if err != nil {
			return nil, fmt.Errorf("raft: resolve advertise addr: %w", err)
		}
	} else {
		advertise = bindAddr
	}

	transport, err := hraft.NewTCPTransport(
		bindAddr.String(),
		advertise,
		cfg.MaxPool,
		cfg.Timeout,
		nil, // logger output (nil = discard)
	)
	if err != nil {
		return nil, fmt.Errorf("raft: create tcp transport: %w", err)
	}

	return transport, nil
}

// NewInmemTransport creates an in-memory transport for testing.
// Returns the transport and its local address.
func NewInmemTransport() (hraft.LoopbackTransport, hraft.ServerAddress) {
	_, transport := hraft.NewInmemTransport("")
	return transport, transport.LocalAddr()
}
