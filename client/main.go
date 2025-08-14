package main

import (
	"bufio"
	"fmt"
	"log"
	mathrand "math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var openConnections int32
var completedRequests int32
var sentRequests int32

func createTCPConn(localIP, remoteAddr string) (net.Conn, error) {
	localAddr, _ := net.ResolveTCPAddr("tcp", localIP+":0")
	dialer := &net.Dialer{
		Timeout:   3 * time.Second,
		KeepAlive: 3 * time.Second,
		LocalAddr: localAddr,
		// Control: func(network, address string, c syscall.RawConn) error {
		// 	return c.Control(func(fd uintptr) {
		// 		err := syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
		// 		if err != nil {
		// 			log.Printf("Error setting SO_REUSEADDR: %v", err)
		// 		}
		// 	})
		// },
	}
	return dialer.Dial("tcp", remoteAddr)
}

func startPersistentConnections(localIP string, num int, wg *sync.WaitGroup, stopCh <-chan struct{}) {
	for i := 0; i < num; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			port := mathrand.Intn(3) + 2002
			address := "127.0.0.1:" + strconv.Itoa(port)

			conn, err := createTCPConn(localIP, address)
			if err != nil {
				log.Printf("[%s] Worker %d: failed to connect: %v", localIP, id, err)
				return
			}
			defer conn.Close()
			// time.Sleep(15 * time.Second)

			atomic.AddInt32(&openConnections, 1)
			defer atomic.AddInt32(&openConnections, -1)

			reader := bufio.NewReader(conn)
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
					atomic.AddInt32(&sentRequests, 1)
					_, err := conn.Write([]byte("PING\n"))
					if err != nil {
						log.Printf("[%s] Worker %d: write error: %v", localIP, id, err)
						return
					}
					_, err = reader.ReadString('\n')
					if err != nil {
						log.Printf("[%s] Worker %d: read error: %v", localIP, id, err)
						return
					}
					atomic.AddInt32(&completedRequests, 1)
				}
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
	//localIPs := []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6"}
	localIPs := []string{"127.0.0.1"}
	numConnectionsPerIP := requestsPerSecond / len(localIPs)
	const duration = 30 * time.Second
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	for _, ip := range localIPs {
		startPersistentConnections(ip, numConnectionsPerIP, &wg, stopCh)
	}

	start := time.Now()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		open := atomic.LoadInt32(&openConnections)
		sent := atomic.SwapInt32(&sentRequests, 0)
		completed := atomic.SwapInt32(&completedRequests, 0)
		log.Printf("Open connections: %d, Sent in last 1s: %d, Completed: %d", open, sent, completed)

		if time.Since(start) >= duration {
			close(stopCh)
			break
		}
	}

	wg.Wait()
	log.Println("Test finished after 15 seconds.")
}
