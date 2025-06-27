package broker

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sahilrana7582/kafka-in-go/internal/producer"
)

func (b *Broker) GetPartition(topic, key string) (Partition, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, exists := b.topics[topic]
	if !exists {
		return Partition{}, fmt.Errorf("topic %s not found", topic)
	}

	partitionCount := len(t.Partitions)
	hash := fnv.New32a()
	hash.Write([]byte(key))
	partitionIndex := int(hash.Sum32()) % partitionCount

	return Partition{
		ID:   partitionIndex,
		Name: fmt.Sprintf("partition-%d.log", partitionIndex),
	}, nil
}

func (b *Broker) AppendMessage(topic, key, message string) error {
	partition, err := b.GetPartition(topic, key)
	if err != nil {
		return err
	}
	partitionPath := filepath.Join("kafka-data", topic, partition.Name)
	cacheKey := partitionPath

	val, ok := b.writeCache.Get(cacheKey)
	if !ok {
		partitionWrite, err := NewPartitionWriter(partitionPath)
		if err != nil {
			return fmt.Errorf("failed to create partition writer for %s: %w", partition.Name, err)
		}
		b.writeCache.Put(cacheKey, partitionWrite)
		val = partitionWrite
	}
	messageFormatted := FormatProductionMessage(key, topic, message)
	writer := val

	if err := writer.Send(messageFormatted); err != nil {
		log.Printf("❌ Failed to send message: %v", err)
	}

	return nil
}

func FormatProductionMessage(key, topic, value string) string {
	unixMillis := time.Now().UnixMilli()
	timestamp := time.Now().Format("2006-01-02T15:04:05.000Z07:00")

	return fmt.Sprintf("%d	|	%s	|	%s	|	%s	|	%s",
		unixMillis,
		timestamp,
		key,
		topic,
		value,
	)
}

func (b *Broker) ReceiveProduceRequest(req producer.ProduceRequest) {
	b.mu.Lock()
	defer b.mu.Unlock()

	wg := sync.WaitGroup{}

	for _, topic := range req.Topics {
		wg.Add(1)
		go func(topic producer.TopicData) {
			defer wg.Done()
			handleTopic(topic)
		}(topic)
	}

	wg.Wait()
	fmt.Printf("✅ Broker ACK: received batch for client\n")
}

func handleTopic(t producer.TopicData) {
	wg := sync.WaitGroup{}

	for partitionID, partition := range t.Partitions {
		wg.Add(1)

		go func(partitionID int32, partition producer.TopicPartitionData) {
			defer wg.Done()

			partitionPath := filepath.Join("kafka-data", t.TopicName, fmt.Sprintf("partition-%d.log", partitionID))

			file, err := os.OpenFile(partitionPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				fmt.Printf("❌ Failed to open partition file %s: %v\n", partitionPath, err)
				return
			}
			defer file.Close()
			fmt.Printf("🛠️ Processing partition %d of topic %s...\n", partitionID, t.TopicName)
			for _, record := range partition.RecordBatch.Records {
				message := fmt.Sprintf("%s	|	|	%s	|	%s	|	%s\n",
					record.Timestamp,
					string(record.Key),
					t.TopicName,
					string(record.Value),
				)
				if _, err := file.WriteString(message); err != nil {
					fmt.Printf("❌ Failed to write record to partition %d of topic %s: %v\n", partitionID, t.TopicName, err)
					return
				}
			}
			if err := file.Sync(); err != nil {
				fmt.Printf("❌ Failed to sync partition file %s: %v\n", partitionPath, err)
				return
			}
			fmt.Printf("✅ Partition %d of topic %s processed successfully.\n", partitionID, t.TopicName)
			// Simulate a delay to mimic real-world processing
			time.Sleep(100 * time.Millisecond)

		}(partitionID, partition)

	}

	wg.Wait()
	fmt.Printf("✅ Broker ACK: received batch for topic %s (Partitions: %d)\n", t.TopicName, len(t.Partitions))
}
