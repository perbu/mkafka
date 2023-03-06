package pkafka

import (
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"time"
)

type buffer struct {
	oldest  time.Time
	records []*kgo.Record
}

// getProbeMessage returns the first message in the buffer and removes it from the buffer:
func (b *buffer) getProbeMessage() (*kgo.Record, error) {
	if len(b.records) == 0 {
		return nil, errors.New("buffer is empty")
	}
	msg := b.records[0]
	b.records = b.records[1:]
	return msg, nil
}

func (b *buffer) size() int {
	return len(b.records)
}

func (b *buffer) add(msg ...*kgo.Record) {
	b.records = append(b.records, msg...)
	if len(b.records) == 1 {
		b.oldest = time.Now()
	}
}

func (b *buffer) clear() {
	b.records = make([]*kgo.Record, 0)
	b.oldest = time.Time{}
}
