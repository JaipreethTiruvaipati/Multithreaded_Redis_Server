//SERIALIZATION LAYER (CONVERTING DATA BETWEEN THE NETWORK BYTES AND GO OBJECTS )
package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// ======================================================================================
// OOP CONCEPT: Polymorphism / Variant Type
// ======================================================================================
// Value acts as a generic container. In a strictly typed language like Go, 
// we use this struct to hold ANY type of Redis data (String, Integer, Array, Error).
type Value struct {
	Typ   string  // Discriminator: "array", "bulk", "string", "null", "error", "int"
	Str   string  // Payload for strings
	Num   int     // Payload for integers (Used for RPUSH response)
	Array []Value // Payload for recursive arrays
}

// ======================================================================================
// SYSTEM DESIGN: Deserialization Layer
// ======================================================================================
// Resp is responsible for parsing the raw TCP byte stream into structured 'Value' objects.
type Resp struct {
	reader *bufio.Reader // Uses Buffered I/O to minimize expensive system calls.
}

// Constructor Pattern
func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

// Read is the main entry point. It implements a "Type Switch" logic.
// It looks at the first byte (Prefix) to decide how to parse the rest.
func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch _type {
	case '*': return r.readArray() // Array
	case '$': return r.readBulk()  // Bulk String
	default:  return Value{}, fmt.Errorf("unknown type: %v", string(_type))
	}
}

// Recursive Parser for Arrays.
// Example: *2\r\n$3\r\nSET\r\n...
func (r *Resp) readArray() (Value, error) {
	v := Value{Typ: "array"}
	
	// 1. Read the size of the array
	lenStr, _, err := r.readLine()
	if err != nil { return v, err }
	length, _ := strconv.Atoi(string(lenStr))

	// 2. Allocate memory
	v.Array = make([]Value, length)
	
	// 3. Loop and Recurse: Call Read() for each element in the array
	for i := 0; i < length; i++ {
		val, err := r.Read()
		if err != nil { return v, err }
		v.Array[i] = val
	}
	return v, nil
}

// Parser for Bulk Strings (Binary Safe).
// Example: $3\r\nFOO\r\n
func (r *Resp) readBulk() (Value, error) {
	v := Value{Typ: "bulk"}
	
	// 1. Read the length
	lenStr, _, err := r.readLine()
	if err != nil { return v, err }
	length, _ := strconv.Atoi(string(lenStr))

	// 2. Read exactly 'length' bytes
	bulk := make([]byte, length)
	r.reader.Read(bulk)
	v.Str = string(bulk)
	
	// 3. Discard the trailing CRLF (\r\n)
	r.readLine() 
	return v, nil
}

// Helper to read until the end of the line (CRLF).
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil { return nil, 0, err }
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' { break }
	}
	return line[:len(line)-2], n, nil
}

// ======================================================================================
// SYSTEM DESIGN: Serialization Layer
// ======================================================================================
// Writer converts Go objects back into the RESP wire format to send to the client.
type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}
func (w *Writer) Write(v Value) error {
	var bytes []byte

	switch v.Typ {
	case "array":
		// <--- NEW: Handle Array Writing
		// Format: *<length>\r\n...elements...
		bytes = []byte("*" + strconv.Itoa(len(v.Array)) + "\r\n")
		if _, err := w.writer.Write(bytes); err != nil {
			return err
		}
		// Recursively write each element in the array
		for _, val := range v.Array {
			if err := w.Write(val); err != nil {
				return err
			}
		}
		return nil // Return early because we handled the writing manually above

	case "string":
		bytes = []byte("+" + v.Str + "\r\n")
	case "bulk":
		bytes = []byte("$" + strconv.Itoa(len(v.Str)) + "\r\n" + v.Str + "\r\n")
	case "null":
		bytes = []byte("$-1\r\n")
	case "error":
		bytes = []byte("-" + v.Str + "\r\n")
	case "null_array":  // <--- ADD THIS CASE
        bytes = []byte("*-1\r\n") // Null Array (for BLPOP timeout)
	case "int":
		bytes = []byte(":" + strconv.Itoa(v.Num) + "\r\n")
	default:
		return fmt.Errorf("unknown type: %v", v.Typ)
	}

	_, err := w.writer.Write(bytes)
	return err
}