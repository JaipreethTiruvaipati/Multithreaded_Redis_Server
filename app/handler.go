//COMMAND IMPLEMENTATION 

package main

import (
	"net"
	"strconv"
	"strings"
	"time"
)

// handleConnection contains the logic for processing client commands.
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close() // Cleanup resource on exit
	
	// OOP Concept: Composition. We attach helpers to the connection.
	resp := NewResp(conn)
	writer := NewWriter(conn)

	for {
		// 1. Deserialization: Parse stream -> Object
		value, err := resp.Read()
		if err != nil { return } // Client disconnected

		if value.Typ != "array" || len(value.Array) == 0 { continue }

		// 2. Command Extraction
		command := strings.ToUpper(value.Array[0].Str)
		args := value.Array[1:]

		// 3. Command Dispatcher (Switch Statement)
		if command == "PING" {
			writer.Write(Value{Typ: "string", Str: "PONG"})
		
		} else if command == "ECHO" {
			if len(args) > 0 {
				writer.Write(Value{Typ: "bulk", Str: args[0].Str})
			}
			} else if command == "RPUSH" { 
				if len(args) < 2 {
					writer.Write(Value{Typ: "error", Str: "ERR wrong number of arguments for 'rpush' command"})
					continue
				}
				
				key := args[0].Str
	
				// CRITICAL SECTION: Lock once for the entire batch
				s.KVMu.Lock()
				
				entry, exists := s.KV[key]
				
				var list []string
				if !exists {
					list = []string{}
				} else {
					// Type Assertion check
					existingList, ok := entry.Value.([]string)
					if !ok {
						s.KVMu.Unlock()
						writer.Write(Value{Typ: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"})
						continue
					}
					list = existingList
				}
	
				// SYSTEM DESIGN OPTIMIZATION: Batch Processing
				// Loop through all remaining arguments (args[1:] means index 1 to end)
				// and append them all under a single lock.
				for _, arg := range args[1:] {
					list = append(list, arg.Str)
				}
				
				// Update the store
				s.KV[key] = Entry{Value: list, Expiry: entry.Expiry}
				s.KVMu.Unlock()
	
				// Return the new length
				writer.Write(Value{Typ: "int", Num: len(list)})

		} else if command == "SET" {
			// ==========================================
			// LOGIC: String Set
			// ==========================================
			if len(args) < 2 {
				writer.Write(Value{Typ: "error", Str: "ERR wrong number of args"})
				continue
			}
			key := args[0].Str
			val := args[1].Str
			var expiry time.Time

			// Parse Options (PX)
			for i := 2; i < len(args); i++ {
				arg := strings.ToUpper(args[i].Str)
				if arg == "PX" {
					i++
					if i >= len(args) { continue }
					ms, _ := strconv.Atoi(args[i].Str)
					// Calculate Absolute Time for Expiry
					expiry = time.Now().Add(time.Duration(ms) * time.Millisecond)
				}
			}

			s.KVMu.Lock()
			// Store as simple string in the interface{}
			s.KV[key] = Entry{Value: val, Expiry: expiry}
			s.KVMu.Unlock()
			writer.Write(Value{Typ: "string", Str: "OK"})

		} else if command == "GET" {
			// ==========================================
			// LOGIC: String Get
			// ==========================================
			if len(args) < 1 {
				writer.Write(Value{Typ: "error", Str: "ERR wrong number of args"})
				continue
			}
			key := args[0].Str

			s.KVMu.Lock()
			entry, ok := s.KV[key]
			
			// Case 1: Key not found
			if !ok {
				s.KVMu.Unlock()
				writer.Write(Value{Typ: "null"})
				continue
			}
			
			// Case 2: Lazy Expiration Check
			if !entry.Expiry.IsZero() && time.Now().After(entry.Expiry) {
				delete(s.KV, key)
				s.KVMu.Unlock()
				writer.Write(Value{Typ: "null"})
				continue
			}

			// Case 3: Type Mismatch Check
			// Since we want to return a string, we assert that Value is a string.
			valStr, ok := entry.Value.(string)
			if !ok {
				s.KVMu.Unlock()
				writer.Write(Value{Typ: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"})
				continue
			}

			s.KVMu.Unlock()
			writer.Write(Value{Typ: "bulk", Str: valStr})
		}
	}
}