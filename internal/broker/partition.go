package broker

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"time"
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
	file, err := os.OpenFile(partitionPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open partition file: %w", err)
	}
	defer file.Close()

	productionGradeMessage := FormatProductionMessage(key, topic, message)
	_, err = file.WriteString(productionGradeMessage)
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	fmt.Printf("📝 Message appended to %s: %s\n", partition.Name, message)
	return nil
}

func FormatProductionMessage(key, topic, value string) string {
	unixMillis := time.Now().UnixMilli()
	timestamp := time.Now().Format("2006-01-02T15:04:05.000Z07:00")

	return fmt.Sprintf("%d	|	%s	|	%s	|	%s	|	%s\n",
		unixMillis,
		timestamp,
		key,
		topic,
		value,
	)
}
