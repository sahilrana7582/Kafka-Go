package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
	err := os.MkdirAll(basePath, 0755)
	if err != nil {
		fmt.Printf("failed to create topic directory: %v\n", err)
		return
	}

	wg := sync.WaitGroup{}

	for i := 0; i < partitions; i++ {
		wg.Add(1)

		go func(partition int, wg *sync.WaitGroup) {
			defer wg.Done()

			filePath := filepath.Join(basePath, fmt.Sprintf("partition-%d.log", partition))

			file, err := os.Create(filePath)
			if err != nil {
				fmt.Printf("failed to create log file for partition %d: %v\n", partition, err)
				return
			}
			defer file.Close()

			fmt.Printf("Created %s\n", filePath)
		}(i+1, &wg)
	}

	wg.Wait()

}
