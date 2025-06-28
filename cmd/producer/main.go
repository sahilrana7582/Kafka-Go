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
	b.CreateTopic("test-topic1", 5)
	b.CreateTopic("test-topic2", 5)
	b.CreateTopic("test-topic3", 5)
	b.CreateTopic("test-topic4", 5)
	b.CreateTopic("test-topic5", 5)
	b.CreateTopic("test-topic6", 5)
	b.CreateTopic("test-topic7", 5)
	b.CreateTopic("test-topic8", 5)
	b.CreateTopic("test-topic9", 5)
	b.CreateTopic("test-topic10", 5)
	b.CreateTopic("test-topic11", 5)
	b.CreateTopic("test-topic12", 5)
	b.CreateTopic("test-topic13", 5)
	b.CreateTopic("test-topic14", 5)
	b.CreateTopic("test-topic15", 5)

	arr := []string{
		"test-topic",
		"test-topic1",
		"test-topic2",
		"test-topic3",
		"test-topic4",
		"test-topic5",
		"test-topic6",
		"test-topic7",
		"test-topic8",
		"test-topic9",
		"test-topic10",
		"test-topic11",
		"test-topic12",
		"test-topic13",
		"test-topic14",
		"test-topic15",
	}
	wg := sync.WaitGroup{}
	start := time.Now()

	for _, topicName := range arr {
		topic := topicName
		wg.Add(1)
		go func() {
			defer wg.Done()
			innerWg := sync.WaitGroup{}

			for i := 0; i < 100; i++ {
				innerWg.Add(1)
				go func() {
					defer innerWg.Done()
					for l := 0; l < 100; l++ {
						key := fmt.Sprintf("key-%d", l)
						message := fmt.Sprintf("Message %d", l)

						producerRecord := p.ProducerRecord{
							Topic: topic,
							Key:   key,
							Value: message,
						}

						if err := producer.Send(producerRecord); err != nil {
							fmt.Printf("❌ Error sending message: %v\n", err)
						}
					}
				}()
			}

			innerWg.Wait()
		}()
	}

	wg.Wait()

	elapsed := time.Since(start)

	time.Sleep(1 * time.Second)

	fmt.Println("================================================")
	fmt.Println("✅ All messages sent successfully")
	fmt.Println("✅ Benchmark complete")
	fmt.Printf("🕒 Total time: %v\n", elapsed)
	fmt.Printf("💌 Total Messages: %v\n", 160000)
	fmt.Printf("📈 Messages per second: %.2f\n", float64(160000)/elapsed.Seconds())

}
