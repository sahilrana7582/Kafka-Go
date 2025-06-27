package main

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

const (
	totalPartition     = 5
	bufferTime         = 2 * time.Second
	maxRecordsPerBatch = 5
)

// Represents a single header within a Kafka record.
type RecordHeader struct {
	Key   []byte
	Value []byte
}

// Represents an individual Kafka message (Record) as seen inside a RecordBatch.
// This is what your application's data (`key`, `value`, `headers`) gets mapped to.
type Record struct {
	// These are deltas relative to the batch's base values
	OffsetDelta int32
	Timestamp   string

	Headers []RecordHeader // Optional, metadata

	Key   []byte // The message key, serialized to bytes
	Value []byte // The message payload, serialized to bytes
}

// Represents a Kafka Record Batch.
type RecordBatch struct {
	// --- Batch Header Fields ---
	BatchLength int32 // Total size of the batch in bytes
	ProducerId  int64
	RecordCount int32 // Number of Records in this batch

	// --- Records within the batch ---
	Records []Record
}

// Represents the data structure for a single partition within a ProduceRequest.
type TopicPartitionData struct {
	PartitionID int32
	RecordBatch RecordBatch
}

// Represents the data structure for a single topic within a ProduceRequest.
type TopicData struct {
	TopicName  string
	Partitions []TopicPartitionData
}

// Represents a Kafka Produce Request.
// This is the actual network request sent from producer to broker.
type ProduceRequest struct {

	// --- Request Body ---
	Acks      int16 // 0, 1, or -1 (all)
	TimeoutMs int32 // Timeout for the request

	Topics []TopicData
}

func main() {
	producer := NewProducer("example-producer")

	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		demo("topic-1", producer)
	}()
	go func() {
		defer wg.Done()
		demo("topic-2", producer)
	}()
	go func() {
		defer wg.Done()
		demo("topic-3", producer)
	}()

	wg.Wait()
	fmt.Println("All demos completed.")
	fmt.Printf("Produced records: %+v\n", producer.TopicMap)
	fmt.Println("Producer finished successfully.")
	fmt.Println("You can now check the produced records in the producer's TopicMap.")

}

type Producer struct {
	Name     string
	TopicMap map[string]TopicData
	mu       sync.Mutex
}

func NewProducer(name string) *Producer {
	return &Producer{
		Name:     name,
		TopicMap: make(map[string]TopicData),
	}
}

func (producer *Producer) AddTopic(topicName string) {
	if _, exists := producer.TopicMap[topicName]; !exists {
		producer.TopicMap[topicName] = TopicData{
			TopicName:  topicName,
			Partitions: []TopicPartitionData{},
		}
	}
}

func (producer *Producer) AddPartition(topicName string, partitionID int32) {
	if topicData, exists := producer.TopicMap[topicName]; exists {
		for _, partition := range topicData.Partitions {
			if partition.PartitionID == partitionID {
				return
			}
		}
		topicData.Partitions = append(topicData.Partitions, TopicPartitionData{
			PartitionID: partitionID,
			RecordBatch: RecordBatch{},
		})
		producer.TopicMap[topicName] = topicData
	} else {
		producer.AddTopic(topicName)
		producer.AddPartition(topicName, partitionID)
	}
}

func (producer *Producer) AddTO(key, topicName string, record Record) error {
	producer.mu.Lock()
	defer producer.mu.Unlock()

	topicData, exists := producer.TopicMap[topicName]
	if !exists {
		return fmt.Errorf("topic %s does not exist", topicName)
	}

	partitionId := partitionKey(key)
	for i := range topicData.Partitions {
		if topicData.Partitions[i].PartitionID == partitionId {
			batch := &topicData.Partitions[i].RecordBatch

			batch.Records = append(batch.Records, record)
			batch.RecordCount++
			batch.BatchLength += int32(len(record.Key) + len(record.Value) + 8)

			// 👇 Persist updated topicData into map
			producer.TopicMap[topicName] = topicData

			fmt.Printf("✅ Partition %d | Record Count: %d, Max: %d\n", partitionId, batch.RecordCount, maxRecordsPerBatch)

			if batch.RecordCount >= maxRecordsPerBatch {
				go producer.FlushBatch(topicName, partitionId)
			}
			return nil
		}
	}
	return fmt.Errorf("partition %d not found in topic %s", partitionId, topicName)
}

func partitionKey(key string) int32 {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	partitionIndex := int(hash.Sum32()) % totalPartition
	return int32(partitionIndex)
}

func demo(topicName string, producer *Producer) {
	fmt.Printf("Demoing topic: %s\n", topicName)
	producer.AddTopic(topicName)

	for i := 0; i < totalPartition; i++ {
		producer.AddPartition(topicName, int32(i))
	}

	fmt.Printf("Added %d partitions to topic %s\n", totalPartition, topicName)
	i := 0
	for {
		record := Record{
			OffsetDelta: int32(i),
			Timestamp:   time.Now().Format("2006-01-02T15:04:05.000Z07:00"),
			Headers: []RecordHeader{
				{Key: []byte("header-key"), Value: []byte("header-value")},
			},
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}

		err := producer.AddTO(fmt.Sprintf("key-%d", i), topicName, record)
		if err != nil {
			fmt.Printf("Failed to produce record: %v\n", err)
		} else {
			fmt.Printf("Produced record with key: %s, value: %s\n", record.Key, record.Value)
		}

		time.Sleep(2 * time.Second)
		producer.mu.Lock()
		i++
		producer.mu.Unlock()
	}
}

func (producer *Producer) FlushBatch(topicName string, partitionId int32) {
	fmt.Printf("💾 FlushBatch() called for topic=%s partition=%d\n", topicName, partitionId)

	producer.mu.Lock()
	defer producer.mu.Unlock()

	topicData := producer.TopicMap[topicName]
	for i := range topicData.Partitions {
		if topicData.Partitions[i].PartitionID == partitionId {
			batch := topicData.Partitions[i].RecordBatch

			if batch.RecordCount == 0 {
				return
			}

			fmt.Printf("\n🚀 Flushing batch: Topic=%s Partition=%d Records=%d\n", topicName, partitionId, batch.RecordCount)
			for _, record := range batch.Records {
				fmt.Printf("   🔸 [Persisted] Key=%s, Value=%s, Timestamp=%s\n", record.Key, record.Value, record.Timestamp)
			}

			// Reset batch
			topicData.Partitions[i].RecordBatch = RecordBatch{}
			producer.TopicMap[topicName] = topicData
			return
		}
	}
}
