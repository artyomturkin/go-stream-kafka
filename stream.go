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
	k := prep()

	k.s = semaphore.NewWeighted(int64(s.c.MaxInflightMessages))
	k.r = kg.NewReader(kg.ReaderConfig{
		Brokers:       s.c.Endpoints,
		Topic:         s.c.Topic,
		GroupID:       group,
		QueueCapacity: s.c.MaxInflightMessages,
		RetentionTime: 7 * 24 * time.Hour,
	})
	k.consumerContext, k.consumerCancel = context.WithCancel(ctx)
	k.consumerWG = &sync.WaitGroup{}
	k.consumerWG.Add(1)

	go k.run()

	return k
}

func (s *kstream) GetProducer(ctx context.Context, group string) stream.Producer {
	k := prep()

	k.w = kg.NewWriter(kg.WriterConfig{
		Brokers: s.c.Endpoints,
		Topic:   s.c.Topic,
	})

	return k
}

// New create new kafka stream
func New(c stream.Config) stream.Stream {
	return &kstream{
		c: c,
	}
}

type kConsumerProducer struct {
	sync.RWMutex
	s *semaphore.Weighted

	closed bool

	r *kg.Reader
	w *kg.Writer

	msgs chan stream.Message
	errs chan error
	done chan struct{}

	errSubs []chan error

	consumerContext context.Context
	consumerCancel  func()
	consumerWG      *sync.WaitGroup
	errsWG          *sync.WaitGroup
}

func prep() *kConsumerProducer {
	k := &kConsumerProducer{
		errs:    make(chan error),
		done:    make(chan struct{}),
		errSubs: []chan error{},
		msgs:    make(chan stream.Message),
		errsWG:  &sync.WaitGroup{},
	}
	k.errsWG.Add(1)
	go k.forwardErrors()
	return k
}

func (k *kConsumerProducer) Close() error {
	k.Lock()

	if !k.closed {
		k.closed = true
		k.Unlock()

		if k.consumerCancel != nil {

			k.consumerCancel()
			k.consumerWG.Wait()
		}

		close(k.errs)
		k.errsWG.Wait()

		close(k.done)
	} else {
		k.Unlock()
	}

	if k.r != nil {
		return k.r.Close()
	}

	return k.w.Close()
}

func (k *kConsumerProducer) Errors() <-chan error {
	errch := make(chan error)

	k.Lock()
	defer k.Unlock()

	if !k.closed {
		k.errSubs = append(k.errSubs, errch)
	} else {
		close(errch)
	}
	return errch
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

func (k *kConsumerProducer) run() {
	defer k.Close()
	defer k.consumerWG.Done()

	err := ping(k.r.Config().Brokers)

	if err != nil {
		k.errs <- err
		return
	}

	for {
		err := k.s.Acquire(k.consumerContext, 1)
		if err != nil {
			k.errs <- err
			return
		}

		m, err := k.r.FetchMessage(k.consumerContext)
		if err != nil {
			k.errs <- err
			return
		}

		var msg interface{}
		err = json.Unmarshal(m.Value, &msg)
		if err != nil {
			k.errs <- err
			k.s.Release(1)
		} else {
			select {
			case k.msgs <- stream.Message{
				Context: stream.SetTrackers(k.consumerContext, m),
				Data:    msg,
			}:
			case <-k.consumerContext.Done():
				return
			}
		}
	}
}

func (k *kConsumerProducer) Publish(ctx context.Context, m interface{}) error {
	k.RLock()
	defer k.RUnlock()

	if k.closed {
		return context.Canceled
	}

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

func (k *kConsumerProducer) forwardErrors() {
	defer k.errsWG.Done()

	for {
		err, more := <-k.errs

		k.RLock()
		subs := k.errSubs[:]
		k.RUnlock()

		if more {
			for _, s := range subs {

				select {
				case s <- err:
				default:
				}

			}
		} else {
			for _, sub := range k.errSubs {
				close(sub)
			}
			return
		}
	}
}

func ping(brokers []string) error {
	var err error
	for _, br := range brokers {
		con, errl := kg.Dial("tcp", br)
		if errl != nil {
			err = errl
			continue
		}
		con.Close()
		break
	}
	return err
}
