package broker

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"strings"
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
	wg := sync.WaitGroup{}

	for _, topic := range req.Topics {
		wg.Add(1)
		go func(topic producer.TopicData) {
			defer wg.Done()
			b.handleTopic(topic) // Call broker's method
		}(*topic)
	}

	wg.Wait()
}

func (b *Broker) handleTopic(t producer.TopicData) {
	wg := sync.WaitGroup{}

	for partitionID, partition := range t.Partitions {
		wg.Add(1)

		go func(partitionID int32, partition *producer.TopicPartitionData) {
			defer wg.Done()

			// Ensure the directory exists
			partitionDir := filepath.Join("kafka-data", t.TopicName)
			if err := os.MkdirAll(partitionDir, 0755); err != nil {
				fmt.Printf("❌ Failed to create directory %s: %v\n", partitionDir, err)
				return
			}

			partitionPath := filepath.Join(partitionDir, fmt.Sprintf("partition-%d.log", partitionID))

			// Open file in append mode. O_CREATE will create if not exists, O_WRONLY for write only.
			file, err := os.OpenFile(partitionPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				fmt.Printf("❌ Failed to open partition file %s: %v\n", partitionPath, err)
				return
			}
			defer file.Close()

			writer := bufio.NewWriter(file)

			var batchContent strings.Builder

			batchContent.Grow(len(partition.RecordBatch.Records) * 100)

			for _, record := range partition.RecordBatch.Records {

				message := fmt.Sprintf("%s | %s | %s | %s\n",
					record.Timestamp, // Assumed to be a formatted string already
					string(record.Key),
					t.TopicName,
					string(record.Value),
				)
				batchContent.WriteString(message)
			}

			// Write the entire batch content to the buffered writer
			if _, err := writer.WriteString(batchContent.String()); err != nil {
				fmt.Printf("❌ Failed to write batch to partition %d of topic %s: %v\n", partitionID, t.TopicName, err)
				return
			}

			// Flush the buffered writer to the underlying file
			if err := writer.Flush(); err != nil {
				fmt.Printf("❌ Failed to flush writer for partition %d of topic %s: %v\n", partitionID, t.TopicName, err)
				return
			}

		}(partitionID, partition)
	}

	wg.Wait()
}
