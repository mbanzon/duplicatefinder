package main

import (
	"crypto/sha256"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type fileContainer struct {
	path string
	hash string
	size int64
}

type fileCounter struct {
	count int64
	size  int64
}

func main() {
	dryRun := flag.Bool("dry", false, "")
	flag.Parse()

	scanningQueue := make(chan fileContainer) // for files to be scanned
	deleteQueue := make(chan fileContainer)   // for files to be deleted
	sumQueue := make(chan fileContainer)      // for files to be hashed

	deletedSumResult := make(chan fileCounter) // for results on deleted files
	totalSumResult := make(chan fileCounter)   // for results on all files

	// setup all the working parts
	walkFn := createWalker(scanningQueue)                 // create filepath.WalkFunc
	createFileScanners(8, scanningQueue, sumQueue)        // create scanners
	startSumHolder(sumQueue, deleteQueue, totalSumResult) // create sum holder
	startDeleter(*dryRun, deleteQueue, deletedSumResult)  // create delete loop

	filepath.Walk(".", walkFn) // start walking

	close(scanningQueue) // close scanning channel when done walking

	deletedSum := <-deletedSumResult // wait for delete result
	totalSum := <-totalSumResult     // wait for total result

	// print stats, total scanned vs. deleted
	fmt.Println("Total files scanned:", totalSum.count)
	fmt.Println("Total filesize: ", totalSum.size)
	fmt.Println("Deleted files:", deletedSum.count)
	fmt.Println("Deleted size:", deletedSum.size)
}

// Creates the WalkFunc that is used by filepath.Walk
func createWalker(scanningQueue chan fileContainer) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() { // check if it is a file
			// if it is a file push the info to channel for scanning
			scanningQueue <- fileContainer{path: path, size: info.Size()}
		}
		return nil
	}
}

// Creates n scanners that take files from the scanning channel and creates
// a (SHA256) hash of their content before putting them on the channel of
// hashed files
func createFileScanners(n int, scanningQeueue chan fileContainer, sumQueue chan fileContainer) {
	var wg sync.WaitGroup    // count the number of scanners we have
	for i := 0; i < n; i++ { // create n scanners
		wg.Add(1) // increase active scanner count
		go func() {
			for file := range scanningQeueue {
				fp, err := os.Open(file.path)
				if err != nil {
					fmt.Println("error opening file:", file)
					fmt.Println(err)
					continue // we proceede to next file on error
				}

				hash := sha256.New()
				if _, err := io.Copy(hash, fp); err != nil {
					fmt.Println("error creating hash for file:", file)
					fmt.Println(err)
					continue // we proceede to next file on error
				}

				fp.Close()

				sum := hash.Sum(nil)
				encodedSum := base64.StdEncoding.EncodeToString(sum)
				file.hash = encodedSum
				sumQueue <- file
			}
			wg.Done() // decrease when this scanner is done
		}()
	}
	go func() {
		wg.Wait()       // wait for all active scanners to finish
		close(sumQueue) // close the channel of sums when done
	}()
}

// starts the function that holds the sums of files and push duplicates.
// having a single loop in a single function removes data race
func startSumHolder(sumQueue chan fileContainer, deleteQueue chan fileContainer, totalSumResult chan fileCounter) {
	go func() {
		var count, size int64                  // used for counting all files
		sums := make(map[string]fileContainer) // holds all sums
		for container := range sumQueue {
			count++
			size += container.size

			if p1, found := sums[container.hash]; found { // if we already know the hash
				dir1 := filepath.Dir(p1.path)
				dir2 := filepath.Dir(container.path)

				if len(dir1) > len(dir2) { // delete the one with the longest path
					deleteQueue <- p1
				} else {
					deleteQueue <- container
					container = p1
				}
			}

			sums[container.hash] = container // store the hash in the map
		}
		// when done (the sum channel has been closed)
		close(deleteQueue)                                      // close the delete channel
		totalSumResult <- fileCounter{count: count, size: size} // and send result on channel
	}()
}

// starts a loop that receives files for deletion.
func startDeleter(dryRun bool, deleteQueue chan fileContainer, deletedSumResult chan fileCounter) {
	go func() {
		var count, size int64 // for counting the deleted files
		for fileToDelete := range deleteQueue {
			count++
			size += fileToDelete.size
			if !dryRun { // if we are actually doing it
				fmt.Println("Deleting file:", fileToDelete.path)
				os.Remove(fileToDelete.path)
			} else { // if we are not actually deleting
				fmt.Println("*Deleting file:", fileToDelete.path)
			}
		}
		// pump result to channel
		deletedSumResult <- fileCounter{count: count, size: size}
	}()
}
