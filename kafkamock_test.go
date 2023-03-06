package pkafka

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"time"
)

type mockClient struct {
	up        bool
	txCounter uint64
	txDelay   time.Duration
	messages  []*kgo.Record
}

func newMockClient() *mockClient {
	return &mockClient{
		up:       true,
		txDelay:  time.Millisecond * 1,
		messages: make([]*kgo.Record, 0, 64),
	}
}

func (m *mockClient) Produce(_ context.Context, record *kgo.Record, promise func(r *kgo.Record, err error)) {
	// this will invoke the callback after a delay.
	// err is set if the kafka client is down.
	up := m.up
	if up {
		m.messages = append(m.messages, record)
		m.txCounter++
	}
	go func() {
		time.Sleep(m.txDelay)
		if up {
			promise(record, nil)
			log.Printf("MOCK: SUCCESS produce wrote message, topic %s, message: %s", record.Topic, string(record.Value))
		} else {
			promise(record, fmt.Errorf("kafka is down"))
			log.Printf("MOCK: FAILED to write message, topic %s, message: %s", record.Topic, string(record.Value))
		}
	}()
}

func (m *mockClient) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	results := make(kgo.ProduceResults, len(records))
	time.Sleep(m.txDelay)
	for i, record := range records {
		if !m.up {
			results[i] = kgo.ProduceResult{
				Record: record,
				Err:    fmt.Errorf("kafka is down"),
			}
			log.Printf("MOCK/sync: FAILED to write message, topic %s, message: %s", record.Topic, string(record.Value))
			continue
		}
		m.messages = append(m.messages, record)
		m.txCounter++
		results[i] = kgo.ProduceResult{
			Record: record,
			Err:    nil,
		}
		log.Printf("MOCK/sync: SUCCESS produce wrote message, topic %s, message: %s", record.Topic, string(record.Value))
	}
	return results
}

func (m *mockClient) Ping(ctx context.Context) error {
	if !m.up {
		return fmt.Errorf("kafka is down")
	}
	return nil
}

func (m *mockClient) BeginTransaction() error {
	log.Println("== BeginTransaction ==")
	if !m.up {
		return fmt.Errorf("kafka is down")
	}
	return nil
}

func (m *mockClient) EndTransaction(ctx context.Context, txn kgo.TransactionEndTry) error {
	log.Println("== EndTransaction ==")
	if !m.up {
		return fmt.Errorf("kafka is down")
	}
	return nil
}

func (m *mockClient) AbortBufferedRecords(ctx context.Context) error {
	log.Println("== AbortBufferedRecords ==")
	if !m.up {
		return fmt.Errorf("kafka is down")
	}
	return nil
}

func (m *mockClient) setDown() {
	m.up = false
}

func (m *mockClient) setUp() {
	m.up = true
}
