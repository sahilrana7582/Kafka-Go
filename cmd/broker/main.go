package main

import (
	"fmt"

	"github.com/sahilrana7582/kafka-in-go/internal/broker"
)

func main() {
	b := broker.NewBroker()

	err := b.CreateTopic("test-topic", 3)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		err = b.AppendMessage("test-topic", fmt.Sprintf("key-%d", i), "Hello Kafka!")
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("Messages appended successfully to topic 'test-topic'.")
	fmt.Println("Broker is running. You can now produce and consume messages.")

}
