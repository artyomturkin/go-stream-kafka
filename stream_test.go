package kafka_test

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/artyomturkin/go-stream"
	kafka "github.com/artyomturkin/go-stream-kafka"
)

var (
	broker = "localhost:9092"
)

func init() {
	if br := os.Getenv("TEST_KAFKA_BROKER"); br != "" {
		broker = br
	}
}

func TestE2E(t *testing.T) {
	ks := kafka.New(stream.Config{
		Endpoints:           []string{broker},
		MaxInflightMessages: 10,
		Topic:               "test-e2e",
	})

	ctx, cancel := context.WithCancel(context.TODO())
	pr := ks.GetProducer(ctx, "test-e2e")
	cs := ks.GetConsumer(ctx, "test-e2e")

	msgs := []string{}
	mu := sync.Mutex{}

	go func() {
		for m := range cs.Messages() {
			if s, ok := m.Data.(string); ok {
				mu.Lock()
				msgs = append(msgs, s)
				mu.Unlock()

				cs.Ack(m.Context)
				continue
			}
			t.Errorf("Received message is not a string: %T | %v", m.Data, m.Data)
		}
	}()

	for i := 0; i < 20; i++ {
		err := pr.Publish(ctx, strconv.Itoa(i))
		if err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	cancel()
	<-cs.Done()
}

func TestProducerTimeOut(t *testing.T) {

	ks := kafka.New(stream.Config{
		Endpoints: []string{"nohost:9092"},
		Topic:     "test-producer-timeout",
	})

	ctx := context.TODO()
	pr := ks.GetProducer(ctx, "test-producer-timeout")

	err := pr.Publish(ctx, strconv.Itoa(1))
	if err == nil {
		t.Fatalf("Expected to timeout before test timeout")
	}
}

func TestConsumerTimeout(t *testing.T) {
	ks := kafka.New(stream.Config{
		Endpoints:           []string{"nohost:9092"},
		MaxInflightMessages: 10,
		Topic:               "test-consumer-timeout",
	})

	ctx := context.TODO()
	cs := ks.GetConsumer(ctx, "test-consumer-timeout")

	err, more := <-cs.Errors()
	if more {

	}
	<-cs.Done()

	if err == nil {
		t.Fatalf("expected and error")
	}
	log.Printf("done")
}
