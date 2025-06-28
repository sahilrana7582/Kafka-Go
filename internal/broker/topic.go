package broker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sahilrana7582/kafka-in-go/internal/producer"
)

func (b *Broker) CreateTopic(name string, partitions int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	if partitions <= 0 {
		partitions = defaultPartitions
	}

	topic := &Topic{
		Name:       name,
		Partitions: make([]Partition, partitions),
	}

	b.topics[name] = topic

	createFilesForTopicAndPartitions(name, partitions, &topic.Partitions)

	return nil
}

func createFilesForTopicAndPartitions(topicName string, partitions int, partitionsSlice *[]Partition) {

	basePath := filepath.Join("kafka-data", topicName)
	if err := os.MkdirAll(basePath, 0755); err != nil {
		fmt.Printf("❌ Failed to create topic directory: %v\n", err)
		return
	}

	wg := sync.WaitGroup{}
	const retryAttempts = 3
	const retryDelayTime = 1 * time.Second
	var mu sync.Mutex

	for i := 0; i < partitions; i++ {
		wg.Add(1)

		go func(partition int, wg *sync.WaitGroup) {
			defer wg.Done()

			partitionName := fmt.Sprintf("partition-%d.log", partition)
			mu.Lock()
			(*partitionsSlice)[partition] = Partition{
				ID:   partition,
				Name: partitionName,
			}
			mu.Unlock()
			partitionPath := filepath.Join(basePath, partitionName)

			file, err := RetryableFileCreate(partitionPath, retryAttempts, retryDelayTime)
			if err != nil {
				fmt.Printf("❌ Failed to create file %s: %v\n", partitionPath, err)
				return
			}
			defer file.Close()

			fmt.Printf("✅ Created partition log: %s\n", partitionPath)
		}(i, &wg)
	}

	wg.Wait()

}

func RetryableFileCreate(filePath string, retries int, delay time.Duration) (*os.File, error) {
	var err error
	var file *os.File

	for attempt := 1; attempt <= retries; attempt++ {
		file, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
		if err == nil {
			return file, nil
		}

		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("file %s already exists", filePath)
		}

		fmt.Printf("[retry %d/%d] error creating %s: %v\n", attempt, retries, filePath, err)
		time.Sleep(delay * time.Duration(attempt))
	}

	return nil, fmt.Errorf("failed to create %s after %d attempts: %w", filePath, retries, err)
}

func (b *Broker) ListTopics() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.topics) == 0 {
		fmt.Println("No topics available.")
		return
	}

	fmt.Println("==== Available topics ====")
	for name, topic := range b.topics {
		fmt.Printf("Topic: %s, Partitions: %d\n", name, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			fmt.Printf("  - Partition ID: %d | Partition Name: %s\n", partition.ID, partition.Name)
		}
	}
}

func (b *Broker) DescribeTopic(name string) (*producer.TopicData, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic, exists := b.topics[name]
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", name)
	}

	topicData := &producer.TopicData{
		TopicName:  topic.Name,
		Partitions: make(map[int32]*producer.TopicPartitionData),
	}
	for _, partition := range topic.Partitions {
		topicData.Partitions[int32(partition.ID)] = &producer.TopicPartitionData{
			PartitionID: int32(partition.ID),
			RecordBatch: producer.RecordBatch{
				RecordCount: 0,
				Records:     []producer.Record{},
			},
		}
	}
	if len(topicData.Partitions) == 0 {
		return nil, fmt.Errorf("topic %s has no partitions", name)
	}
	return topicData, nil
}
