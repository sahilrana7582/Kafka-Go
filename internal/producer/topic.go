package producer

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"time"
)

func (producer *Producer) AddTopic(topicName string) {
	if _, exists := producer.TopicMap[topicName]; !exists {
		producer.TopicMap[topicName] = TopicData{
			TopicName:  topicName,
			Partitions: make(map[int32]TopicPartitionData),
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
		partitionNew := TopicPartitionData{
			PartitionID: partitionID,
			RecordBatch: RecordBatch{},
		}

		topicData.Partitions[partitionID] = partitionNew
		producer.TopicMap[topicName] = topicData
	} else {
		producer.AddTopic(topicName)
		producer.AddPartition(topicName, partitionID)
	}
}

func (producer *Producer) AddToPartition(key, topicName string, record Record) error {
	producer.mu.Lock()
	defer producer.mu.Unlock()

	topicData, exists := producer.TopicMap[topicName]
	if !exists {
		return fmt.Errorf("topic %s does not exist", topicName)
	}

	partitionId := partitionKey(key)
	if partitionData, exists := topicData.Partitions[partitionId]; exists {

		partitionData.RecordBatch.Records = append(partitionData.RecordBatch.Records, record)
		partitionData.RecordBatch.RecordCount++

		topicData.Partitions[partitionId] = partitionData
		producer.TopicMap[topicName] = topicData
		fmt.Printf("Current record count in partition %d: %d\n", partitionId, partitionData.RecordBatch.RecordCount)
		if partitionData.RecordBatch.RecordCount >= totalPartition {
			go producer.FlushBatch(topicName, partitionId)
		}

	}
	return nil
}

func partitionKey(key string) int32 {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	partitionIndex := int(hash.Sum32()) % totalPartition
	return int32(partitionIndex)
}

func Demo(topicName string, producer *Producer) {
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

		err := producer.AddToPartition(fmt.Sprintf("key-%d", i), topicName, record)
		if err != nil {
			fmt.Printf("Failed to produce record: %v\n", err)
		} else {
			fmt.Printf("Produced record with key: %s, value: %s\n", record.Key, record.Value)
		}

		time.Sleep(500 * time.Millisecond)
		producer.mu.Lock()
		i++
		producer.mu.Unlock()
	}
}

func (producer *Producer) FlushBatch(topicName string, partitionId int32) {
	producer.mu.Lock()
	defer producer.mu.Unlock()

	file, err := os.OpenFile("internal/producer/flush.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("❌ Failed to open flush log file: %v", err)
		return
	}
	defer file.Close()

	logger := log.New(file, "FLUSH: ", log.LstdFlags|log.Lshortfile)

	// Get topic data
	topicData, ok := producer.TopicMap[topicName]
	if !ok {
		logger.Printf("❌ Topic %s not found\n", topicName)
		return
	}

	// Get partition data
	partitionData, ok := topicData.Partitions[partitionId]
	if !ok {
		logger.Printf("❌ Partition %d not found in topic %s\n", partitionId, topicName)
		return
	}

	if partitionData.RecordBatch.RecordCount == 0 {
		logger.Printf("⚠️  No records to flush for topic %s partition %d\n", topicName, partitionId)
		return
	}

	// ✅ Construct ProduceRequest
	produceReq := ProduceRequest{
		Acks:      1,
		TimeoutMs: 5000,
		Topics: []TopicData{
			{
				TopicName: topicName,
				Partitions: map[int32]TopicPartitionData{
					partitionId: {
						PartitionID: partitionId,
						RecordBatch: partitionData.RecordBatch,
					},
				},
			},
		},
	}

	logger.Printf("🚀 Flushing %d records from topic %s partition %d\n", partitionData.RecordBatch.RecordCount, topicName, partitionId)
	for _, record := range partitionData.RecordBatch.Records {
		logger.Printf("Flushing record: key=%s, value=%s\n", record.Key, record.Value)
	}

	// 🔄 Send it to the Broker (if you want to simulate this)
	if producer.Broker != nil {
		producer.Broker.ReceiveProduceRequest(produceReq)
	}

	// 🔄 Clear batch after flushing
	partitionData.RecordBatch.Records = nil
	partitionData.RecordBatch.RecordCount = 0
	topicData.Partitions[partitionId] = partitionData
	producer.TopicMap[topicName] = topicData

	logger.Printf("✅ Flushed and cleared batch for topic %s partition %d\n", topicName, partitionId)
	logger.Println("=========================================")
}
