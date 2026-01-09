package main

import (
	"net"
	"strconv"
	"strings"
	"time"
)
// Helper to check if any client is blocked on this key.
// Returns true if the value was sent to a waiting client (hijacked).
func (s *Server) dispatch(key string, value string) bool {
	s.BlockedMu.Lock()
	defer s.BlockedMu.Unlock()

	// Check if anyone is waiting
	clients, ok := s.BlockedClients[key]
	if !ok || len(clients) == 0 {
		return false // No waiters
	}

	// FIFO: Wake up the first client in line
	client := clients[0]
	
	// Update the list (remove the first client)
	s.BlockedClients[key] = clients[1:]
	if len(s.BlockedClients[key]) == 0 {
		delete(s.BlockedClients, key)
	}

	// Send data to the client's channel
	// We send [key, value] because BLPOP response includes the key name
	client.Ch <- []string{key, value}
	
	return true // Data was consumed!
}
// handleConnection manages the lifecycle of a single client connection.
// SYSTEM DESIGN: This runs in its own Goroutine (Lightweight Thread), 
// allowing the server to handle thousands of concurrent clients.
func (s *Server) handleConnection(conn net.Conn) {
	// RESOURCE MANAGEMENT: Ensure the TCP connection is closed when this function exits.
	// This prevents file descriptor leaks in the OS.
	defer conn.Close()

	// OOP CONCEPT: Composition
	// We create helper objects to handle the specific tasks of Parsing (Resp) and Writing (Writer).
	resp := NewResp(conn)
	writer := NewWriter(conn)

	// MAIN EVENT LOOP: Keep reading commands from this client indefinitely.
	for {
		// 1. DESERIALIZATION (Input Processing)
		// Read raw bytes from the network and convert them into a structured 'Value' object.
		value, err := resp.Read()
		if err != nil {
			// If reading fails (e.g., client disconnected), exit the loop and close connection.
			return
		}

		// VALIDATION: Ensure the request is a valid RESP Array.
		if value.Typ != "array" || len(value.Array) == 0 {
			continue
		}

		// 2. COMMAND PARSING
		// Extract the command name (normalized to Uppercase) and its arguments.
		command := strings.ToUpper(value.Array[0].Str)
		args := value.Array[1:]

		// 3. COMMAND DISPATCHER (Switch Logic)
		// OOP: This acts like a Factory or Strategy pattern, routing to specific logic.

		if command == "PING" {
			// Simple health check. Returns a Simple String "PONG".
			writer.Write(Value{Typ: "string", Str: "PONG"})

		} else if command == "ECHO" {
			// Returns the argument back to the client.
			if len(args) > 0 {
				writer.Write(Value{Typ: "bulk", Str: args[0].Str})
			}

		} else if command == "RPUSH" {
			// ============================================================
			// COMMAND: RPUSH key element [element ...]
			// Append one or more values to a list.
			// ============================================================
			
			// VALIDATION: Check if we have at least a key and one value.
			if len(args) < 2 {
				writer.Write(Value{Typ: "error", Str: "ERR wrong number of arguments for 'rpush' command"})
				continue
			}

			key := args[0].Str

			// CONCURRENCY CONTROL: Write Lock (Lock)
			// We use Lock() because we are MODIFYING the state (appending to list).
			// This pauses all other readers/writers for this map.
			s.KVMu.Lock()

			// Check if key already exists in the store.
			entry, exists := s.KV[key]

			var list []string
			if !exists {
				// CASE A: New List. Create an empty slice.
				list = []string{}
			} else {
				// CASE B: Existing Key. Check Type.
				// GO SPECIFIC: Type Assertion. We must verify the interface{} holds a []string.
				existingList, ok := entry.Value.([]string)
				if !ok {
					// Error: The key exists but holds a String (from SET), not a List.
					s.KVMu.Unlock() // CRITICAL: Always release lock before returning!
					writer.Write(Value{Typ: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"})
					continue
				}
				list = existingList
			}

			// Inside RPUSH logic...
        // ... (Lock KVMu, check type, etc.) ...

			// OLD LOOP:
			// for _, arg := range args[1:] {
			//     list = append(list, arg.Str)
			// }

			// NEW LOOP: Event-Driven Logic
			for _, arg := range args[1:] {
				// 1. Check if anyone is waiting for this key
				// If dispatch returns true, the value goes directly to the waiting client.
				// It effectively "skips" the database storage.
				if s.dispatch(key, arg.Str) {
					continue 
				}

				// 2. If no one is waiting, append to list normally
				list = append(list, arg.Str)
			}

			// Update the store with the new list.
			// We preserve the old Expiry time (if any).
			s.KV[key] = Entry{Value: list, Expiry: entry.Expiry}
			
			// Release the lock immediately after the critical section is done.
			s.KVMu.Unlock()

			// RESP PROTOCOL: Return the new length of the list as an Integer.
			writer.Write(Value{Typ: "int", Num: len(list)})

		} else if command == "SET" {
			// ============================================================
			// COMMAND: SET key value [PX milliseconds]
			// Set a string value with optional expiration.
			// ============================================================
			if len(args) < 2 {
				writer.Write(Value{Typ: "error", Str: "ERR wrong number of arguments for 'set' command"})
				continue
			}

			key := args[0].Str
			val := args[1].Str
			var expiry time.Time // Default zero value means "no expiry"

			// OPTION PARSING: Iterate to find "PX" flag
			for i := 2; i < len(args); i++ {
				arg := strings.ToUpper(args[i].Str)
				if arg == "PX" {
					i++ // Move to the argument after PX (the milliseconds value)
					if i >= len(args) { break }
					
					// Parse string to integer
					ms, _ := strconv.Atoi(args[i].Str)
					
					// SYSTEM DESIGN: Absolute Time vs Relative Time
					// Redis stores the exact timestamp when the key dies (Now + TTL).
					expiry = time.Now().Add(time.Duration(ms) * time.Millisecond)
				}
			}

			// CONCURRENCY CONTROL: Write Lock
			s.KVMu.Lock()
			// Store the string. 'interface{}' automatically wraps the string type.
			s.KV[key] = Entry{Value: val, Expiry: expiry}
			s.KVMu.Unlock()

			writer.Write(Value{Typ: "string", Str: "OK"})

		} else if command == "GET" {
			// ============================================================
			// COMMAND: GET key
			// Retrieve a string value.
			// ============================================================
			if len(args) < 1 {
				writer.Write(Value{Typ: "error", Str: "ERR wrong number of arguments for 'get' command"})
				continue
			}
			key := args[0].Str

			// CONCURRENCY CONTROL: Write Lock
			// We use Lock() instead of RLock() because we might need to DELETE the key
			// if it has expired (Lazy Expiration is a write operation).
			s.KVMu.Lock()
			
			entry, ok := s.KV[key]
			
			// Case 1: Key does not exist
			if !ok {
				s.KVMu.Unlock()
				writer.Write(Value{Typ: "null"}) // Return "$-1\r\n"
				continue
			}

			// Case 2: Lazy Expiration Check
			// If Expiry is set (not zero) AND current time is past expiry...
			if !entry.Expiry.IsZero() && time.Now().After(entry.Expiry) {
				delete(s.KV, key) // Cleanup the dead key
				s.KVMu.Unlock()
				writer.Write(Value{Typ: "null"})
				continue
			}

			// Case 3: Type Safety Check
			// Ensure we are returning a String, not a List.
			valStr, ok := entry.Value.(string)
			if !ok {
				s.KVMu.Unlock()
				writer.Write(Value{Typ: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"})
				continue
			}

			s.KVMu.Unlock()
			writer.Write(Value{Typ: "bulk", Str: valStr})

		} else if command == "LRANGE" {
			// ============================================================
			// COMMAND: LRANGE key start end
			// Retrieve a sub-range of elements from a list.
			// ============================================================
			if len(args) < 3 {
				writer.Write(Value{Typ: "error", Str: "ERR wrong number of arguments for 'lrange' command"})
				continue
			}

			key := args[0].Str
			// Parse indices from string to int
			start, err1 := strconv.Atoi(args[1].Str)
			end, err2 := strconv.Atoi(args[2].Str)

			if err1 != nil || err2 != nil {
				writer.Write(Value{Typ: "error", Str: "ERR value is not an integer or out of range"})
				continue
			}

			// CONCURRENCY CONTROL: Read Lock (RLock)
			// We only READ data here. RLock allows multiple clients to run LRANGE 
			// at the exact same time without waiting for each other. High performance!
			s.KVMu.RLock()
			
			entry, exists := s.KV[key]

			// Case 1: Key does not exist -> Return empty array
			if !exists {
				s.KVMu.RUnlock() // Always unlock
				writer.Write(Value{Typ: "array", Array: []Value{}})
				continue
			}

			// Case 2: Type Check
			// Ensure the value is actually a List ([]string)
			val, ok := entry.Value.([]string)
			if !ok {
				s.KVMu.RUnlock()
				writer.Write(Value{Typ: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"})
				continue
			}

			length := len(val)

			// --- INDEX NORMALIZATION LOGIC ---
			
			// 1. Handle Negative Start Index (e.g., -1 is last element)
			if start < 0 {
				start = length + start
				if start < 0 {
					start = 0 // Clamp to 0 if result is still negative
				}
			}

			// 2. Handle Negative End Index
			if end < 0 {
				end = length + end
				if end < 0 {
					end = 0 // Clamp to 0 if result is still negative
				}
			}

			// 3. Bounds Checking (Redis Rules)
			if start >= length {
				// Start is beyond the list -> Return empty
				s.KVMu.RUnlock()
				writer.Write(Value{Typ: "array", Array: []Value{}})
				continue
			}

			if end >= length {
				end = length - 1 // Clamp end to the actual last index
			}

			if start > end {
				// Invalid range -> Return empty
				s.KVMu.RUnlock()
				writer.Write(Value{Typ: "array", Array: []Value{}})
				continue
			}

			// --- SLICING ---
			// Create the subset slice.
			// Go slicing [start:limit] is exclusive at limit, so we use end+1.
			rawSlice := val[start : end+1]

			// Convert []string to []Value so our Writer can understand it.
			respArray := make([]Value, len(rawSlice))
			for i, s := range rawSlice {
				respArray[i] = Value{Typ: "bulk", Str: s}
			}

			s.KVMu.RUnlock()
			writer.Write(Value{Typ: "array", Array: respArray})
			} else if command == "LPUSH" {
				// ==========================================
				// LOGIC: Left Push (Prepend)
				// ==========================================
				if len(args) < 2 {
					writer.Write(Value{Typ: "error", Str: "ERR wrong number of arguments for 'lpush' command"})
					continue
				}
	
				key := args[0].Str
	
				// CRITICAL SECTION: Lock once for the whole batch
				s.KVMu.Lock()
	
				entry, exists := s.KV[key]
	
				var list []string
				if !exists {
					list = []string{}
				} else {
					existingList, ok := entry.Value.([]string)
					if !ok {
						s.KVMu.Unlock()
						writer.Write(Value{Typ: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"})
						continue
					}
					list = existingList
				}
	
				// LOOP: Iterate through args and Prepend them one by one.
				// Example: LPUSH key A B C
				// 1. Prepend A -> [A, ...]
				// 2. Prepend B -> [B, A, ...]
				// 3. Prepend C -> [C, B, A, ...]
				// --- CHANGED LOGIC START ---
			
			// Iterate through all arguments (e.g., LPUSH key val1 val2)
			for _, arg := range args[1:] {
				
				// 1. Check for waiting clients (BLPOP)
				// If dispatch returns true, the value was hijacked by a waiting client.
				if s.dispatch(key, arg.Str) {
					continue // Skip adding to the list!
				}

				// 2. If no one is waiting, prepend to the list normally
				// append([]string{val}, list...) creates a new slice with val at the front
				list = append([]string{arg.Str}, list...)
			}
			
			// --- CHANGED LOGIC END ---
	
				// Update Store
				s.KV[key] = Entry{Value: list, Expiry: entry.Expiry}
				s.KVMu.Unlock()
	
				// Return the new length
				writer.Write(Value{Typ: "int", Num: len(list)})
	} else if command == "LLEN" {
		// ==========================================
		// LOGIC: List Length
		// ==========================================
		if len(args) < 1 {
			writer.Write(Value{Typ: "error", Str: "ERR wrong number of arguments for 'llen' command"})
			continue
		}

		key := args[0].Str

		// Use Read Lock (RLock) for high concurrency
		s.KVMu.RLock()
		entry, exists := s.KV[key]

		// Case 1: Key does not exist -> Return 0
		if !exists {
			s.KVMu.RUnlock()
			writer.Write(Value{Typ: "int", Num: 0})
			continue
		}

		// Case 2: Wrong Type Check
		list, ok := entry.Value.([]string)
		if !ok {
			s.KVMu.RUnlock()
			writer.Write(Value{Typ: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"})
			continue
		}

		// Case 3: Return Length
		length := len(list)
		s.KVMu.RUnlock()

		writer.Write(Value{Typ: "int", Num: length})
		}  else if command == "LPOP" {
			// ==========================================
			// LOGIC: Left Pop (Single or Multiple)
			// ==========================================
			if len(args) < 1 {
				writer.Write(Value{Typ: "error", Str: "ERR wrong number of arguments for 'lpop' command"})
				continue
			}

			key := args[0].Str
			count := 1
			hasCountArg := false

			// 1. Check for Optional Count Argument
			if len(args) > 1 {
				c, err := strconv.Atoi(args[1].Str)
				if err != nil || c < 0 {
					writer.Write(Value{Typ: "error", Str: "ERR value is not an integer or out of range"})
					continue
				}
				count = c
				hasCountArg = true
			}

			// CRITICAL SECTION: Write Lock
			s.KVMu.Lock()
			entry, exists := s.KV[key]

			// Case 1: Key does not exist
			if !exists {
				s.KVMu.Unlock()
				if hasCountArg {
					// LPOP key count -> Returns Empty Array if missing
					writer.Write(Value{Typ: "array", Array: []Value{}})
				} else {
					// LPOP key -> Returns Null if missing
					writer.Write(Value{Typ: "null"})
				}
				continue
			}

			// Case 2: Wrong Type Check
			list, ok := entry.Value.([]string)
			if !ok {
				s.KVMu.Unlock()
				writer.Write(Value{Typ: "error", Str: "WRONGTYPE Operation against a key holding the wrong kind of value"})
				continue
			}

			// Case 3: Calculate actual number to pop
			// If count > length, we just pop the whole list
			if count > len(list) {
				count = len(list)
			}

			// Perform the Pop
			// Go Slicing: list[:count] gives the first 'count' elements
			poppedElements := list[:count]
			newList := list[count:]

			// Update Store
			// If list is empty now, we usually delete the key, but updating it to empty slice is also fine for this stage
			if len(newList) == 0 {
				delete(s.KV, key)
			} else {
				s.KV[key] = Entry{Value: newList, Expiry: entry.Expiry}
			}
			
			s.KVMu.Unlock()

			// Case 4: Formulate Response
			if hasCountArg {
				// Return Array (even if count is 1)
				values := make([]Value, len(poppedElements))
				for i, v := range poppedElements {
					values[i] = Value{Typ: "bulk", Str: v}
				}
				writer.Write(Value{Typ: "array", Array: values})
			} else {
				// Return Single Bulk String
				if len(poppedElements) == 0 {
					writer.Write(Value{Typ: "null"})
				} else {
					writer.Write(Value{Typ: "bulk", Str: poppedElements[0]})
				}
			}
			} else if command == "BLPOP" {
				// ==========================================
				// LOGIC: Blocking Pop
				// ==========================================
				// args format: [key1, timeout] (Simplifying to single key for now)
				if len(args) < 2 {
					writer.Write(Value{Typ: "error", Str: "ERR wrong number of arguments for 'blpop' command"})
					continue
				}
	
				key := args[0].Str
				timeoutStr := args[len(args)-1].Str // Last arg is always timeout
				timeoutSec, err := strconv.ParseFloat(timeoutStr, 64)
				if err != nil {
					writer.Write(Value{Typ: "error", Str: "ERR timeout is not a float or out of range"})
					continue
				}
	
				// Phase 1: Try Non-Blocking Pop First
				s.KVMu.Lock()
				// (Simple implementation: check single key. Real Redis loops all keys)
				entry, exists := s.KV[key]
				
				// If data exists, pop immediately (Act like LPOP)
				if exists {
					list, ok := entry.Value.([]string)
					if ok && len(list) > 0 {
						val := list[0]
						newList := list[1:]
						if len(newList) == 0 {
							delete(s.KV, key)
						} else {
							s.KV[key] = Entry{Value: newList, Expiry: entry.Expiry}
						}
						s.KVMu.Unlock()
	
						// Return Array: [key, value]
						writer.Write(Value{Typ: "array", Array: []Value{
							{Typ: "bulk", Str: key},
							{Typ: "bulk", Str: val},
						}})
						continue
					}
				}
				s.KVMu.Unlock()
	
				// Phase 2: Block (Wait)
				// Create a channel to wait on
				resultCh := make(chan []string)
				
				// Register ourselves
				s.BlockedMu.Lock()
				s.BlockedClients[key] = append(s.BlockedClients[key], &BlockedClient{Ch: resultCh})
				s.BlockedMu.Unlock()
	
				// Calculate timeout channel
				var timeoutCh <-chan time.Time
				if timeoutSec > 0 {
					timeoutCh = time.After(time.Duration(timeoutSec) * time.Second)
				}
				// If timeout is 0, timeoutCh remains nil (blocks forever, which is what we want)
	
				// Wait for event OR timeout
				select {
				case res := <-resultCh:
					// Wake up! We got data from RPUSH
					writer.Write(Value{Typ: "array", Array: []Value{
						{Typ: "bulk", Str: res[0]}, // Key
						{Typ: "bulk", Str: res[1]}, // Value
					}})
				case <-timeoutCh:
					// Timed out. Remove ourselves from the list.
					s.BlockedMu.Lock()
					clients := s.BlockedClients[key]
					// Filter out our channel
					var remaining []*BlockedClient
					for _, c := range clients {
						if c.Ch != resultCh {
							remaining = append(remaining, c)
						}
					}
					s.BlockedClients[key] = remaining
					s.BlockedMu.Unlock()
	
					// Return Null Array
					writer.Write(Value{Typ: "null_array"})
				}
			}
}
}	