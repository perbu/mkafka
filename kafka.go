// Package pkafka provides a kafka producer that tries really hard never to lose messages.
// It'll retry messages that fail to send, and will buffer messages to disk, so we're sure we're not loosing them.
package pkafka

/*
 Messages flow like this:

 When the producer is called it'll just push the message into a buffered channel. This will make it async.
 On the other side of the channel there is a goroutine that will read the messages, in order, and send them to kafka.
 It'll batch up messages and send them to kafka every second.

 If a batch fails to send, it'll retry it for about 20 seconds. If it still fails it'll assume kafka is down and
 will switch to storing messages on GCS.

 It'll keep the first message in memory, and will keep on retrying endlessly.

 If it succeeds it'll remove the first message from disk and then re-ingest the next message from disk.

*/

import (
	"context"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type Client struct {
	// kafka *kgo.Client
	kafka             abstractKafka
	failed            bool
	failedSince       time.Time
	lastAttempt       time.Time
	buffer            *buffer
	syncInterval      time.Duration
	msgCount          *uint64
	bufferCh          chan *kgo.Record
	probeMessage      *kgo.Record // this is a message we'll use to see if kafka is back up again, after being down.
	logger            *log.Logger
	downProbeInterval time.Duration
}

// abstractKafka is an interface that abstracts the kafka client.
// we use the interface to mock the kafka client in tests.
type abstractKafka interface {
	Produce(ctx context.Context, record *kgo.Record, promise func(r *kgo.Record, err error))
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults
	Ping(ctx context.Context) error
	BeginTransaction() error
	EndTransaction(ctx context.Context, txn kgo.TransactionEndTry) error
	AbortBufferedRecords(ctx context.Context) error
}

var (
	// ErrKafkaDown is returned when kafka is down.
	ErrKafkaDown = fmt.Errorf("kafka is down")
)

// New creates a new kafka client meant for production usage.
// kafka must be up for the client to initialize.
func New() (*Client, error) {
	brokersStr, ok := os.LookupEnv("KAFKA_BROKERS")
	if !ok {
		brokersStr = "localhost:9092"
	}
	brokers := strings.Split(brokersStr, ",")

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.TransactionalID("pkafka"),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)),
	)
	if err != nil {
		return nil, fmt.Errorf("initializing kafka client: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	if err = kafkaClient.Ping(pingCtx); err != nil {
		return nil, fmt.Errorf("pinging kafka: %w", err)
	}

	return newClient(kafkaClient)
}

// new implements New and allows you ot customize it, for instance for testing purposes.
func newClient(kafkaClient abstractKafka) (*Client, error) {

	bu := &buffer{
		records: make([]*kgo.Record, 0),
	}
	logger := log.New(log.Writer(), "pkafka", log.Flags())
	s := Client{
		kafka:             kafkaClient,
		buffer:            bu,
		msgCount:          new(uint64),
		bufferCh:          make(chan *kgo.Record, 1000),
		failed:            false,
		logger:            logger,
		syncInterval:      time.Second * 1,
		downProbeInterval: time.Second * 30,
	}
	return &s, nil
}

// Run starts the goroutine that will flush the buffer to kafka or GCS.
// this will reduce the need to locking, and will make the producer async.
func (c *Client) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.syncInterval)
	defer ticker.Stop()

	for {
		// this is a select with priority to the buffer channel.
		// this to ensure that we don't lose messages.
		select {
		case msg := <-c.bufferCh:
			c.buffer.add(msg)
		default:
			select {
			case msg := <-c.bufferCh:
				c.buffer.add(msg)
			case <-ctx.Done():
				var err error
				c.logger.Printf("final flush: %d messages in buffer", len(c.buffer.records))
				err = c.flush()
				if err != nil {
					return fmt.Errorf("final flush: %w", err)
				}
				return nil
			case <-ticker.C:
				_ = c.flush()
			}
		}
	}
}

func (c *Client) Produce(msg kgo.Record) error {
	c.bufferCh <- &msg
	return nil
}

func (c *Client) Ping() error {
	// make ctx with timeout of 5 seconds:
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	return c.kafka.Ping(ctx)
}

// flush will attempt to flush the buffer to kakfa, or if kafka is down, to GCS.
// if that'll fail too then maybe we should abort?
func (c *Client) flush() error {
	// if kafka is up and there's nothing to flush, we'll just return.
	if !c.failed && c.buffer.size() == 0 {
		return nil
	}

	// If kafka is known to be down, we only probe it every downProbeInterval:
	if c.failed && time.Since(c.lastAttempt) > c.downProbeInterval {
		c.lastAttempt = time.Now()
		err := c.probeKafka()
		if err != nil {
			c.logger.Printf("flush: kafka is still down")
			return ErrKafkaDown
		}
		if err == nil {
			c.failed = false
			c.logger.Println("flush: kafka is back up")
		}
	}
	// if kafka is down, we can't flush.
	if c.failed {
		return ErrKafkaDown
	}

	// here we assume kafka is up:

	err := c.flushToKafka()
	// if the flush succeeded, we'll just return, nothing more to do.
	if err != nil {
		c.failed = true
		c.failedSince = time.Now()
		c.logger.Println("flush: kafka went down")
	}
	return nil
}

// probeKafka will grab the first message from the buffer and send it to kafka.
func (c *Client) probeKafka() error {
	// if we don't have a probe message, we'll create one.
	if c.probeMessage == nil {
		var err error
		c.probeMessage, err = c.buffer.getProbeMessage()
		if err != nil {
			return fmt.Errorf("probeKafka: %w", err)
		}
	}
	// if we have a probe message, we'll send it to kafka.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	res := c.kafka.ProduceSync(ctx, c.probeMessage)
	err := res.FirstErr()
	if err != nil {
		return fmt.Errorf("probeKafka: %w", err)
	}
	return nil
}

// flushToKafka will flush the buffer to kafka.
// if kafka is down, it'll return an error and leave the messages in the buffer.
func (c *Client) flushToKafka() error {
	msgCount := c.buffer.size()
	c.logger.Printf("flushing %d msgs to kafka", msgCount)
	start := time.Now()
	defer c.logger.Printf("flushing to kafka took %v", time.Since(start))
	msgs := c.buffer.records
	retCh := make(chan error, msgCount)
	err := c.kafka.BeginTransaction()
	if err != nil {
		c.logger.Println("beginning transaction: ", err)
		return fmt.Errorf("beginning transaction: %w", err)
	}

	for i, msg := range msgs {
		c.logger.Println("submitting msg to kafka: ", i)
		c.kafka.Produce(context.TODO(), msg,
			func(r *kgo.Record, err error) { retCh <- err })
	}
	// all messages are sent, we're waiting for the return values.
	errs := make([]error, 0, msgCount)

	for i := 0; i < msgCount; i++ {
		err := <-retCh
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}
	c.logger.Println("kafka: send: done")

	// if we had any errors, we'll ask kafka to rollback and just leave the messages in the buffer
	// to be flushed to GCS.
	if len(errs) != 0 {
		c.logger.Println("kafka: send: some messages failed. tx rollback.")
		rollback(context.Background(), c.kafka)
		return errors.Join(errs...)
	}

	// all messages were sent successfully.
	// we can commit the transaction.
	err = c.kafka.EndTransaction(context.Background(), kgo.TryCommit)
	if err != nil {
		c.logger.Println("committing tx: ", err)
		rollback(context.Background(), c.kafka)
		return fmt.Errorf("committing tx: %w", err)
	}
	c.buffer.clear()
	return nil
}

func rollback(ctx context.Context, client abstractKafka) {
	if err := client.AbortBufferedRecords(ctx); err != nil {
		fmt.Printf("error aborting buffered records: %v\n", err) // this only happens if ctx is canceled
		return
	}
	if err := client.EndTransaction(ctx, kgo.TryAbort); err != nil {
		fmt.Printf("error rolling back transaction: %v\n", err)
		return
	}
	fmt.Println("transaction rolled back")
}

func (c *Client) GetMsgCount() uint64 {
	return atomic.LoadUint64(c.msgCount)
}

func (c *Client) Status() (string, error) {
	healthy := c.failed
	if !healthy {
		return fmt.Sprintf("Kafka down since %v", time.Since(c.failedSince)), ErrKafkaDown
	}
	return "Hunky dory", nil
}
