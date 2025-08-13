package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Job struct {
	ID int
}

var JobQueue = make(chan Job, 100)

var completedRequests int32
var sentRequests int32

func createTCPConn(addr string) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		Control: func(network, address string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				err := syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
				if err != nil {
					log.Printf("Error setting SO_REUSEADDR: %v", err)
				}
			})
		},
	}
	return dialer.Dial("tcp", addr)
}

func StartWorkerPool(num int, wg *sync.WaitGroup) {
	for i := 0; i < num; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for job := range JobQueue {
				atomic.AddInt32(&sentRequests, 1)
				conn, err := createTCPConn("127.0.0.1:2002")
				if err != nil {
					log.Printf("Worker %d: Request %d failed to connect: %v", id, job.ID, err)
					continue
				}

				reader := bufio.NewReader(conn)

				// to tcp
				_, err = conn.Write([]byte("PING\n"))
				if err != nil {
					log.Printf("Worker %d: Request %d write error: %v", id, job.ID, err)
					conn.Close()
					continue
				}
				_, err = reader.ReadString('\n')
				if err != nil {
					log.Printf("Worker %d: Request %d read error: %v", id, job.ID, err)
					conn.Close()
					continue
				}
				atomic.AddInt32(&completedRequests, 1)
				conn.Close()
			}
		}(i)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <requests_per_second>")
		os.Exit(1)
	}
	requestsPerSecond, err := strconv.Atoi(os.Args[1])
	if err != nil || requestsPerSecond <= 0 {
		fmt.Println("Please provide a valid number of requests per second")
		os.Exit(1)
	}

	var wg sync.WaitGroup
	go StartWorkerPool(64, &wg)

	const duration = 15 * time.Second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	start := time.Now()

	go func() {
		tickerCount := time.NewTicker(1 * time.Second)
		defer tickerCount.Stop()
		for range tickerCount.C {
			sent := atomic.SwapInt32(&sentRequests, 0)
			completed := atomic.SwapInt32(&completedRequests, 0)
			log.Printf("=================================")
			log.Printf("Requests sent in last 1 second: %d, Completed: %d", sent, completed)
			if time.Since(start) >= duration {
				break
			}
		}
	}()

	for i := 0; time.Since(start) < duration; i++ {
		<-ticker.C
		for j := 0; j < requestsPerSecond; j++ {
			JobQueue <- Job{ID: (i * requestsPerSecond) + j}
		}
	}

	close(JobQueue)
	wg.Wait()
	fmt.Printf("Completed send requests")
}
