package main

import (
	"fmt"
	"log"
	"os"

	"primality_afs/client" // Adjust this if your module name is different
)

func main() {
	// 1. Initialize the AFS Client (Connecting to localhost, caching in /tmp/afs_worker1)
	log.Println("--- Starting Test Worker ---")
	afs, err := client.InitAFS("localhost:50051", "/tmp/afs_worker1")
	if err != nil {
		log.Fatalf("Failed to init AFS: %v", err)
	}

	// 2. Test fetching an input file (Cache Miss -> Download)
	fmt.Println("\n>>> Opening Input File...")
	inFd, err := afs.AFS_Open("input_dataset_001.txt", os.O_RDONLY)
	if err != nil {
		log.Fatalf("Open failed: %v", err)
	}

	// 3. Read the data locally
	buf := make([]byte, 1024)
	n, _ := afs.AFS_Read(inFd, buf)
	fmt.Printf("Data read from AFS:\n%s\n", string(buf[:n]))

	// 4. Close the input file (Should NOT trigger an upload because it's read-only)
	afs.AFS_Close(inFd)
	fmt.Println("Input file closed.")

	// 5. Test creating and writing to the output file
	fmt.Println("\n>>> Writing to Output File...")
	outFd, err := afs.AFS_Open("primes.txt", os.O_CREATE|os.O_WRONLY)
	if err != nil {
		log.Fatalf("Failed to open primes.txt: %v", err)
	}

	// Pretend we did the math and found these primes
	mockPrimes := []byte("17\n23\n")
	afs.AFS_Write(outFd, mockPrimes)
	fmt.Println("Wrote mock primes locally.")

	// 6. Close the output file (Should trigger the gRPC upload!)
	fmt.Println("\n>>> Closing Output File (Triggering Upload)...")
	err = afs.AFS_Close(outFd)
	if err != nil {
		log.Fatalf("Failed to close/upload: %v", err)
	}
	
	fmt.Println("--- Test Complete! ---")
}