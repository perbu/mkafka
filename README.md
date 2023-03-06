# mkafka

This package implements a buffered writer for Kafka. It'll try to write to Kafka asynchronously, 
and if it fails, it'll retry until it succeeds. It'll persist the in the buffer until it succeeds.

It uses the amazing franz-go package to do the heavy lifting.


## Usage

```go
package main

import (
	"context"
	"github.com/perbu/mkafka"
	"log"
	"os"
	"os/signal"
	"sync"
)

func main() {
	// create a client.
	client, err := mkafka.New()
	if err != nil {
		panic(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		// write to kafka, will always succeed
		err := client.Run(ctx)
		if err != nil {
			log.Println("client.Run() failed:", err)
		}
	}()
	// write to kafka, will always succeed
	err = client.Write(ctx, "test", []byte("hello world"))
	if err != nil {
        log.Println("client.Write() failed:", err)
    }
	cancel()
	wg.Wait()
}

```