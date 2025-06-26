package broker

import (
	"fmt"
	"hash/fnv"
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
	cacheKey := fmt.Sprintf("%s:%s", topic, key)

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
	writer := val.(*PartitionWriter)
	writer.MessagesChan <- messageFormatted

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
