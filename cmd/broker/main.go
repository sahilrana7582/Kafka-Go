package main

import "github.com/sahilrana7582/kafka-in-go/internal/broker"

func main() {
	b := broker.NewBroker()

	err := b.CreateTopic("test-topic-1", 3)
	if err != nil {
		panic(err)
	}
}
