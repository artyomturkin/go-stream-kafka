# go-stream-kafka
Kafka stream provider for go-stream package

Add to `go.mod`

```
require github.com/artyomturkin/go-stream-kafka v1.1.0
```

Then import package in code:

```go
import (
    "github.com/artyomturkin/go-stream"
    kafka "github.com/artyomturkin/go-stream-kafka"
)

func example() {
    kstream := kafka.New(stream.Config{
        Endpoints:           []string{"broker0:9092", "broker1:9092"},
        MaxInflightMessages: 10,
        Topic:               "test-topic",
    })

    ctx, cancel := context.WithCancel(context.TODO())
    
    producer := kstream.GetProducer(ctx, "test-group")
    consumer := kstream.GetConsumer(ctx, "test-group")
}
```