package main

import (
	"fmt"
	"sync"

	"github.com/sahilrana7582/kafka-in-go/internal/broker"
	p "github.com/sahilrana7582/kafka-in-go/internal/producer"
)

func main() {
	b := broker.NewBroker()
	producer := p.NewProducer("example-producer", b)

	b.CreateTopic("test-topic", 5)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.Demo("test-topic", producer)
	}()

	wg.Wait()
	fmt.Println("All demos completed.")
	fmt.Printf("Produced records: %+v\n", producer.TopicMap)
	fmt.Println("Producer finished successfully.")
	fmt.Println("You can now check the produced records in the producer's TopicMap.")

}
