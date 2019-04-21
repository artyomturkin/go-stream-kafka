package kafka_test

import (
	"context"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

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
	log.Printf("Start test: GOMAXPROCS: %d", runtime.GOMAXPROCS(-1))

	ks := kafka.New(stream.Config{
		Endpoints:           []string{broker},
		MaxInflightMessages: 10,
		Topic:               "test-e2e",
	})

	ctx, cancel := context.WithCancel(context.TODO())
	pr := ks.GetProducer(ctx, "test-e2e")
	pr.Publish(ctx, "create topic")
	time.Sleep(1 * time.Second)

	wg := &sync.WaitGroup{}
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func(wg *sync.WaitGroup, i int) {
			log.Printf("Publishing %d", i)
			err := pr.Publish(ctx, strconv.Itoa(i))
			if err != nil {
				t.Errorf("Failed to publish message %d: %v", i, err)
			}
			wg.Done()
		}(wg, i)
	}

	wg.Wait()

	cs := ks.GetConsumer(ctx, "test-e2e")

	msgs := []string{}
	mu := sync.Mutex{}

	go func() {
		for {
			err, ok := <-cs.Errors()
			if !ok {
				return
			}
			if err != context.Canceled {
				t.Errorf("Consumer error: %v", err)
			}
		}
	}()

	counter := 0
	log.Printf("Start consuming")
	for m := range cs.Messages() {
		counter++

		log.Printf("Got message: %v", m.Data)
		if s, ok := m.Data.(string); ok {
			mu.Lock()
			msgs = append(msgs, s)
			mu.Unlock()

			cs.Ack(m.Context)
			if counter == 20 {
				break
			}
			continue
		}
		if counter == 20 {
			break
		}
		t.Errorf("Received message is not a string: %T | %v", m.Data, m.Data)
	}
	log.Printf("finished consuming")

	log.Printf("Cancel")

	cancel()
	<-cs.Done()

	log.Printf("Run checks")
	if len(msgs) != 20 {
		t.Errorf("expected 20 msgs, got %d", len(msgs))
	}
}

func TestProducerTimeout(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
