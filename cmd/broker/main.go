package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sahilrana7582/kafka-in-go/internal/broker"
)

const (
	topicName             = "test-topic"
	numPartitions         = 5
	numConcurrentWriters  = 10
	messagesPerWriter     = 200
	totalExpectedMessages = numConcurrentWriters * messagesPerWriter
	delayBetweenSends     = 1 * time.Millisecond
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	fmt.Println("--- Starting Hardcore Broker Test ---")

	b := broker.NewBroker()

	fmt.Printf("Creating topic '%s' with %d partitions...\n", topicName, numPartitions)
	err := b.CreateTopic(topicName, numPartitions)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	fmt.Println("Topic created successfully.")

	fmt.Printf("Starting %d concurrent writers, each sending %d messages...\n", numConcurrentWriters, messagesPerWriter)
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numConcurrentWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < messagesPerWriter; j++ {
				key := fmt.Sprintf("key-%d-%d", writerID, j)
				message := fmt.Sprintf("Message from writer %d, seq %d: %s", writerID, j, time.Now().Format(time.RFC3339Nano))

				err := b.AppendMessage(topicName, key, message)
				if err != nil {
					log.Printf("Writer %d, message %d: Failed to append message '%s': %v", writerID, j, key, err)
				}
			}
			fmt.Printf("✅✅✅✅Writer %d finished sending %d messages.\n", writerID, messagesPerWriter)
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)
	fmt.Printf("\nAll writers finished. Total messages attempted: %d. Time taken: %s\n", totalExpectedMessages, duration)

	time.Sleep(5 * time.Second)

	fmt.Println("Verifying message counts in partition files...")
	actualMessagesWritten := broker.TotalWritten
	if actualMessagesWritten == totalExpectedMessages {
		fmt.Println("Test PASSED: All expected messages were written successfully!")
	} else {
		fmt.Printf("Test FAILED: Mismatch in message count. Expected %d, Got %d.\n", totalExpectedMessages, actualMessagesWritten)
	}

	fmt.Println("\n--- Hardcore Broker Test Finished ---")
}
