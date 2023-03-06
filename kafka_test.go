package pkafka

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync"
	"testing"
	"time"
)

func magicMessage(id int) kgo.Record {
	return kgo.Record{
		Topic: "magic",
		Value: []byte(fmt.Sprintf("magic message %d", id)),
	}
}

func TestEverythingIsFine(t *testing.T) {
	mock := newMockClient()
	cl, err := newClient(mock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := sync.WaitGroup{}
	wg.Add(1)
	errCh := make(chan error, 10)
	go func() {
		defer wg.Done()
		errCh <- cl.Run(ctx)
	}()
	for i := 0; i < 10; i++ {
		err = cl.Produce(magicMessage(i))
		if err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	wg.Wait()
	err = <-errCh
	if err != nil {
		t.Fatal(err)
	}
	if mock.txCounter != 10 {
		t.Errorf("mock counter: %d", mock.txCounter)
	}
	if len(mock.messages) != 10 {
		t.Errorf("mock messages stored: %d", len(mock.messages))
	}
	for i := 0; i < 10; i++ {
		if string(mock.messages[i].Value) != fmt.Sprintf("magic message %d", i) {
			t.Errorf("mock message %d: %s", i, string(mock.messages[i].Value))
		}
	}
}

func TestFailureAndRecovery(t *testing.T) {
	mock := newMockClient()
	cl, err := newClient(mock)
	cl.syncInterval = time.Millisecond // sync every millisecond.
	cl.downProbeInterval = time.Millisecond * 3

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := sync.WaitGroup{}
	wg.Add(1)
	errCh := make(chan error, 10)
	go func() {
		defer wg.Done()
		errCh <- cl.Run(ctx)
	}()
	mock.setDown() // fail kafka
	for i := 0; i < 10; i++ {
		err = cl.Produce(magicMessage(i))
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Millisecond * 5)
	mock.setUp() // recover kafka
	time.Sleep(time.Millisecond * 10)
	cancel()
	wg.Wait()
	err = <-errCh
	if err != nil {
		t.Fatal(err)
	}
	if mock.txCounter != 10 {
		t.Errorf("mock counter: %d", mock.txCounter)
	}
	if len(mock.messages) != 10 {
		t.Errorf("mock messages stored: %d", len(mock.messages))
	}
}

func TestSlow(t *testing.T) {
	mock := newMockClient()
	cl, err := newClient(mock)
	cl.syncInterval = time.Millisecond // sync every millisecond.
	cl.downProbeInterval = time.Millisecond * 3

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := sync.WaitGroup{}
	wg.Add(1)
	errCh := make(chan error, 10)
	go func() {
		defer wg.Done()
		errCh <- cl.Run(ctx)
	}()
	for i := 0; i < 10; i++ {
		err = cl.Produce(magicMessage(i))
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond * 1)
	}
	cancel()
	wg.Wait()
	err = <-errCh
	if err != nil {
		t.Fatal(err)
	}
	if mock.txCounter != 10 {
		t.Errorf("mock counter: %d", mock.txCounter)
	}
	if len(mock.messages) != 10 {
		t.Errorf("mock messages stored: %d", len(mock.messages))
	}
	for i := 0; i < 10; i++ {
		if string(mock.messages[i].Value) != fmt.Sprintf("magic message %d", i) {
			t.Errorf("mock message %d: %s", i, string(mock.messages[i].Value))
		}
	}
}
