package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/artyomturkin/go-stream"
	"github.com/hashicorp/go-hclog"
	k "github.com/segmentio/kafka-go"
	"golang.org/x/sync/semaphore"
)

type streamController struct {
	s                  *semaphore.Weighted
	uc                 *stream.WireConfig
	breakOnFormatError bool
	brokers            []string
	topic              string
	log                hclog.Logger
}

// New create new stream from config
func New(c stream.Config) stream.Stream {
	return &streamController{
		s:                  semaphore.NewWeighted(int64(c.MaxInflightMessages)),
		uc:                 c.WireConfig,
		breakOnFormatError: c.ForwardUnmarshalErrors,
		brokers:            c.Endpoints,
		topic:              c.Topic,
		log:                c.Logger.Named("kafka").With("topic", c.Topic),
	}
}

// Ensure that streamController implements stream.Stream interface
var _ stream.Stream = (*streamController)(nil)

func (s *streamController) GetConsumer(group string) stream.Consumer {
	r := k.NewReader(k.ReaderConfig{
		Brokers:       s.brokers,
		Topic:         s.topic,
		GroupID:       group,
		QueueCapacity: 1,
		RetentionTime: 7 * 24 * time.Hour,
	})
	return &consumer{
		s:                  s.s,
		r:                  r,
		uc:                 s.uc,
		breakOnFormatError: s.breakOnFormatError,
		log:                s.log.With("direction", "consumer", "group", group),
	}
}

func (s *streamController) GetProducer(group string) stream.Producer {
	w := k.NewWriter(k.WriterConfig{
		Brokers: s.brokers,
		Topic:   s.topic,
	})
	return &producer{
		w:   w,
		uc:  s.uc,
		log: s.log.With("direction", "producer", "group", group),
	}
}

type consumer struct {
	s                  *semaphore.Weighted
	r                  *k.Reader
	uc                 *stream.WireConfig
	breakOnFormatError bool
	log                hclog.Logger
}

func (c *consumer) Read(ctx context.Context) (*stream.Message, error) {
	log.Printf("[KAFKA] Read - acquire thread")
	err := c.s.Acquire(ctx, 1)
	if err != nil {
		return nil, err
	}

	c.log.Debug("thread acquired")

	for {
		c.log.Debug("read message")

		m, err := c.r.FetchMessage(ctx)
		if err != nil {
			c.log.Error("read error", "error", err)
			return nil, err
		}

		msg, err := stream.UnmarshalMessage(m.Value, c.uc)
		if err == nil {
			c.log.Debug("forward message", "partition", m.Partition, "eventID", msg.ID)
			msg.StreamMeta = m
			return msg, nil
		}

		c.log.Error("unmarshaling error", "error", err)
		if c.breakOnFormatError {
			return nil, err
		}

		c.log.Debug("skipping")
		err = c.r.CommitMessages(ctx, m)
		if err != nil {
			c.log.Error("skipping error", "error", err)
			return nil, fmt.Errorf("failed to commit skipped message")
		}
	}
}
func (c *consumer) Ack(ctx context.Context, m *stream.Message) error {
	c.log.Debug("release thread", "eventID", m.ID)
	defer c.s.Release(1)
	if msg, ok := m.StreamMeta.(k.Message); ok {
		err := c.r.CommitMessages(ctx, msg)
		if err != nil {
			c.log.Error("failed to ack message", "eventID", m.ID, "error", err)
			return err
		}
		c.log.Debug("ack success", "eventID", m.ID)
		return nil
	}

	c.log.Error("StreamMeta is not formated correctly", "eventID", m.ID)
	return errors.New("StreamMeta is not formated correctly")
}

func (c *consumer) Nack(context.Context, *stream.Message) error {
	c.log.Debug("release thread")
	c.s.Release(1)
	return nil
}

func (c *consumer) Close() error {
	return c.r.Close()
}

type producer struct {
	w   *k.Writer
	uc  *stream.WireConfig
	log hclog.Logger
}

func (p *producer) Publish(ctx context.Context, m *stream.Message) error {
	l := p.log.With("eventID", m.ID)
	data, err := stream.MarshalMessage(m, p.uc)
	if err != nil {
		l.Error("failed to marshal message", "error", err)
		return err
	}

	err = p.w.WriteMessages(ctx, k.Message{Value: data.([]byte)})
	if err != nil {
		l.Error("failed to publish message", "error", err)
		return err
	}

	l.Debug("publish success")
	return nil
}

func (p *producer) Close() error {
	return p.w.Close()
}
