package main

import "github.com/sahilrana7582/kafka-in-go/internal/broker"

func main() {
	b := broker.NewBroker()

	err := b.CreateTopic("test-topic", 3)
	if err != nil {
		panic(err)
	}
	// Create another topic with a different number of partitions
	err = b.CreateTopic("another-topic", 5)
	if err != nil {
		panic(err)
	}
}
