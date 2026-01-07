//STORAGE ENGINE AND DATA MODELS
package main

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// ======================================================================================
// OOP CONCEPT: Data Transfer Object (DTO) & Interface
// ======================================================================================
// Entry represents a single record in our database.
type Entry struct {
	// We use 'interface{}' here for Polymorphism.
	// It allows 'Value' to hold EITHER a 'string' (from SET) OR a '[]string' (from RPUSH).
	Value  interface{} 
	
	// Expiry Metadata for TTL (Time To Live).
	Expiry time.Time
}

// ======================================================================================
// OOP CONCEPT: Encapsulation
// ======================================================================================
// Server groups the network listener, configuration, and the data store into one object.
type Server struct {
	ListenAddr string
	Listener   net.Listener
	
	// SYSTEM DESIGN: In-Memory Storage
	// A Hash Map provides O(1) average time complexity for lookups.
	KV         map[string]Entry
	
	// SYSTEM DESIGN: Concurrency Control
	// Go maps are NOT thread-safe. We use a RWMutex to prevent Race Conditions
	// when multiple clients try to read/write at the same time.
	KVMu       sync.RWMutex
}

// Constructor Pattern
func NewServer(listenAddr string) *Server {
	return &Server{
		ListenAddr: listenAddr,
		KV:         make(map[string]Entry), // Initialize map to avoid panic
	}
}

// Start initiates the TCP server.
func (s *Server) Start() error {
	// System Call: Bind to port
	ln, err := net.Listen("tcp", s.ListenAddr)
	if err != nil {
		return err
	}
	s.Listener = ln
	fmt.Printf("Redis server running on %s\n", s.ListenAddr)
	
	return s.acceptLoop()
}

// The main loop that waits for new connections.
func (s *Server) acceptLoop() error {
	for {
		// System Call: Accept (Blocking call)
		conn, err := s.Listener.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}
		
		// SYSTEM DESIGN: Concurrency Model
		// Spawn a lightweight Goroutine for each client. 
		// This enables the server to handle thousands of concurrent connections.
		go s.handleConnection(conn)
	}
}