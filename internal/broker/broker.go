package broker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	defaultPartitions = 3
)

type Partition struct {
	ID   int
	Name string
}

type Topic struct {
	Name       string
	Partitions []Partition
}

type Broker struct {
	topics map[string]*Topic
	mu     sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		topics: make(map[string]*Topic),
	}
}

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

	createFilesForTopicAndPartitions(name, partitions)

	return nil
}

func createFilesForTopicAndPartitions(topicName string, partitions int) {

	basePath := filepath.Join("kafka-data", topicName)
	if err := os.MkdirAll(basePath, 0755); err != nil {
		fmt.Printf("❌ Failed to create topic directory: %v\n", err)
		return
	}

	wg := sync.WaitGroup{}
	const retryAttempts = 3
	const retryDelayTime = 1 * time.Second

	for i := 0; i < partitions; i++ {
		wg.Add(1)

		go func(partition int, wg *sync.WaitGroup) {
			defer wg.Done()

			partitionPath := filepath.Join(basePath, fmt.Sprintf("partition-%d", partition))

			file, err := RetryableFileCreate(partitionPath, retryAttempts, retryDelayTime)
			if err != nil {
				fmt.Printf("❌ Failed to create file %s: %v\n", partitionPath, err)
				return
			}
			defer file.Close()

			fmt.Printf("✅ Created partition log: %s\n", partitionPath)
		}(i+1, &wg)
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
