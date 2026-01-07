package main

import (
	"fmt"
	"os"
)

func main() {
	// Standard output for logging (useful for debugging tests)
	fmt.Println("Logs from your program will appear here!")

	// Initialize Server object
	server := NewServer("0.0.0.0:6379")
	
	// Start the main event loop
	if err := server.Start(); err != nil {
		fmt.Println("Server failed to start:", err)
		os.Exit(1)
	}
}