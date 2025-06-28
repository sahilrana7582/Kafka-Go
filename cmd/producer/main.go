package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/sahilrana7582/kafka-in-go/internal/broker"
	p "github.com/sahilrana7582/kafka-in-go/internal/producer"
)

func main() {
	b := broker.NewBroker()
	producer := p.NewProducer("example-producer", b)

	b.CreateTopic("test-topic", 5)
	wg := sync.WaitGroup{}

	start := time.Now()

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for l := 0; l < 10; l++ {
				key := fmt.Sprintf("key-%d", l)
				message := fmt.Sprintf("Message %d", l)

				producerRecord := p.ProducerRecord{
					Topic: "test-topic",
					Key:   key,
					Value: message,
				}

				err := producer.Send(producerRecord)
				if err != nil {
					fmt.Printf("Error sending message: %v\n", err)
					continue
				}
				fmt.Printf("Produced record with key: %s, value: %s to topic: %s\n", key, message, producerRecord.Topic)
			}
		}()
	}

	wg.Wait()

	elapsed := time.Since(start)

	fmt.Println("✅ Benchmark complete")
	fmt.Printf("🕒 Total time: %v\n", elapsed)
	fmt.Printf("📈 Messages per second: %.2f\n", float64(1000)/elapsed.Seconds())

}
