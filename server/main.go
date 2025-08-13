package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

// pingCounter là bộ đếm an toàn để theo dõi số request PING nhận được
var pingCounter int32

func handleConnection(c net.Conn) {
	defer c.Close()
	reader := bufio.NewReader(c)
	for {
		netData, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("Connection error: %v", err)
			return
		}
		if strings.TrimSpace(string(netData)) == "STOP" {
			log.Printf("Exiting connection")
			return
		}
		if strings.TrimSpace(string(netData)) == "PING" {
			atomic.AddInt32(&pingCounter, 1)
			_, err := c.Write([]byte("PONG\n"))
			if err != nil {
				log.Printf("Write error: %v", err)
				return
			}
		}
	}
}

func main() {
	PORT := ":2002"
	l, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatalf("Listen error: %v", err)
	}
	defer l.Close()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			count := atomic.SwapInt32(&pingCounter, 0) // Lấy giá trị và reset
			log.Printf("=================================")
			log.Printf("PING requests received in last 1 second: %d", count)
		}
	}()

	log.Printf("Starting TCP server on :2002")
	for {
		c, err := l.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go handleConnection(c)
	}
}
