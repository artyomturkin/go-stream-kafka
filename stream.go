package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/artyomturkin/go-stream"
	kg "github.com/segmentio/kafka-go"
	"golang.org/x/sync/semaphore"
)

type kstream struct {
	c stream.Config
}

var _ stream.Stream = &kstream{}
var _ stream.Producer = &kConsumerProducer{}
var _ stream.Consumer = &kConsumerProducer{}

func (s *kstream) GetConsumer(ctx context.Context, group string) stream.Consumer {
	r := kg.NewReader(kg.ReaderConfig{
		Brokers:       s.c.Endpoints,
		Topic:         s.c.Topic,
		GroupID:       group,
		QueueCapacity: s.c.MaxInflightMessages,
		RetentionTime: 7 * 24 * time.Hour,
	})
	return &kConsumerProducer{
		s: semaphore.NewWeighted(int64(s.c.MaxInflightMessages)),
		r: r,
	}
}

func (s *kstream) GetProducer(ctx context.Context, group string) stream.Producer {
	w := kg.NewWriter(kg.WriterConfig{
		Brokers: s.c.Endpoints,
		Topic:   s.c.Topic,
	})
	return &kConsumerProducer{
		w:        w,
		producer: true,
	}
}

// New create new kafka stream
func New(c stream.Config) stream.Stream {
	return &kstream{}
}

type kConsumerProducer struct {
	sync.Mutex
	s *semaphore.Weighted

	running  bool
	producer bool

	r *kg.Reader
	w *kg.Writer

	msgs chan stream.Message
	errs chan error
	done chan struct{}
}

func (k *kConsumerProducer) Close() error {
	defer close(k.errs)

	if k.producer {
		defer close(k.done)
	}

	return k.r.Close()
}

func (k *kConsumerProducer) Errors() <-chan error {
	return k.errs
}

func (k *kConsumerProducer) Done() <-chan struct{} {
	return k.done
}

func (k *kConsumerProducer) Ack(ctx context.Context) error {
	defer k.s.Release(1)

	tracks := stream.GetTrackers(ctx)

	msgs := []kg.Message{}
	for _, t := range tracks {
		if msg, ok := t.(kg.Message); ok {
			msgs = append(msgs, msg)
		}
	}

	if err := k.r.CommitMessages(ctx, msgs...); err != nil {
		return err
	}

	return nil
}

func (k *kConsumerProducer) Nack(ctx context.Context) error {
	k.s.Release(1)
	return nil
}

func (k *kConsumerProducer) Messages() <-chan stream.Message {
	return k.msgs
}

func (k *kConsumerProducer) run(ctx context.Context) {
	k.Lock()
	if k.running {
		k.Unlock()
		return
	}

	k.running = true

	k.msgs = make(chan stream.Message)
	defer close(k.msgs)

	k.errs = make(chan error)

	k.done = make(chan struct{})
	defer close(k.done)

	k.Unlock()

	for {
		err := k.s.Acquire(ctx, 1)
		if err != nil {
			k.errs <- err
			return
		}

		m, err := k.r.FetchMessage(ctx)
		if err != nil {
			k.errs <- err
			return
		}

		var msg map[string]interface{}
		err = json.Unmarshal(m.Value, &msg)
		if err != nil {
			k.errs <- err
			k.s.Release(1)
		} else {
			k.msgs <- stream.Message{
				Context: stream.SetTrackers(ctx, m),
				Data:    msg,
			}
		}
	}
}

func (k *kConsumerProducer) Publish(ctx context.Context, m interface{}) error {
	b, err := json.Marshal(m)
	if err != nil {
		k.errs <- err
		return err
	}

	err = k.w.WriteMessages(ctx, kg.Message{Value: b})
	if err != nil {
		k.errs <- err
		return err
	}

	return nil
}
