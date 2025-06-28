package producer

import (
	"fmt"
	"hash/fnv"
	"time"
)

func (p *Producer) Send(record ProducerRecord) error {
	if record.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}

	if record.Value == "" {
		return fmt.Errorf("value cannot be empty")
	}

	if record.Key == "" {
		record.Key = generateRandomKey()
	}

	topicData, err := p.getMetaData(record.Topic)
	if err != nil {
		return fmt.Errorf("failed to get metadata for topic %s: %w", record.Topic, err)
	}

	if record.Partition == nil {
		partitionID := partitionKey(record.Key, len(p.TopicMap[record.Topic].Partitions))
		record.Partition = &partitionID
	}

	totalPartition := len(topicData.Partitions)
	newRecord := Record{
		OffsetDelta: int32(len(topicData.Partitions[*record.Partition].RecordBatch.Records)),
		Timestamp:   time.Now().Format("2006-01-02T15:04:05.000Z07:00"),
		Headers: []RecordHeader{
			{Key: []byte("header-key"), Value: []byte(record.Key)},
			{Key: []byte("header-value"), Value: []byte("header-value")},
		},
		Key:   []byte(record.Key),
		Value: []byte(record.Value),
	}
	err = p.AddToPartition(record.Key, topicData, totalPartition, newRecord)
	if err != nil {
		return fmt.Errorf("failed to add record to partition: %w", err)
	}
	fmt.Printf("Produced record with key: %s, value: %s to topic: %s, partition: %d\n", record.Key, record.Value, record.Topic, *record.Partition)
	return nil
}

func (producer *Producer) AddToPartition(key string, topicData *TopicData, totalPart int, record Record) error {
	// producer.mu.Lock()
	// defer producer.mu.Unlock()

	partitionId := partitionKey(key, totalPart)
	if partitionData, exists := topicData.Partitions[partitionId]; exists {
		partitionData.mu.Lock()
		defer partitionData.mu.Unlock()

		partitionData.RecordBatch.Records = append(partitionData.RecordBatch.Records, record)
		partitionData.RecordBatch.RecordCount++

		fmt.Printf("Current record count in partition %d: %d\n", partitionId, partitionData.RecordBatch.RecordCount)
		if partitionData.RecordBatch.RecordCount >= 5 {
			go producer.FlushBatch(topicData, partitionId)
		}

	}
	return nil
}

func partitionKey(key string, totalPartition int) int32 {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	partitionIndex := int(hash.Sum32()) % totalPartition
	return int32(partitionIndex)
}

func (producer *Producer) FlushBatch(topicData *TopicData, partitionId int32) {
	// producer.mu.Lock()
	// defer producer.mu.Unlock()

	topicName := topicData.TopicName

	// Get partition data
	partitionData, ok := topicData.Partitions[partitionId]
	if !ok {
		fmt.Printf("❌ Partition %d not found in topic %s\n", partitionId, topicName)
		return
	}

	partitionData.mu.Lock()
	defer partitionData.mu.Unlock()
	if partitionData.RecordBatch.RecordCount == 0 {
		fmt.Printf("⚠️  No records to flush for topic %s partition %d\n", topicName, partitionId)
		return
	}

	// ✅ Construct ProduceRequest
	produceReq := ProduceRequest{
		Acks:      1,
		TimeoutMs: 5000,
		Topics: []*TopicData{
			{
				TopicName: topicName,
				Partitions: map[int32]*TopicPartitionData{
					partitionId: {
						PartitionID: partitionId,
						RecordBatch: partitionData.RecordBatch,
					},
				},
			},
		},
	}

	// 🔄 Send it to the Broker (if you want to simulate this)
	if producer.Broker != nil {
		producer.Broker.ReceiveProduceRequest(produceReq)
	}

	// 🔄 Clear batch after flushing
	partitionData.RecordBatch.Records = make([]Record, 0)
	partitionData.RecordBatch.RecordCount = 0

	fmt.Printf("✅ Flushed and cleared batch for topic %s partition %d\n", topicName, partitionId)
	fmt.Println("=========================================")
}

func (p *Producer) getMetaData(topicName string) (*TopicData, error) {

	p.mu.RLock()
	topicData, exists := p.TopicMap[topicName]
	p.mu.RUnlock()
	if !exists {
		tempTopicData, err := p.Broker.DescribeTopic(topicName)
		if err != nil {
			return nil, fmt.Errorf("failed to describe topic %s: %w", topicName, err)
		}
		topicData = tempTopicData
		p.mu.Lock()
		p.TopicMap[topicName] = topicData
		p.mu.Unlock()
	}

	return topicData, nil
}

func generateRandomKey() string {
	key := fmt.Sprintf("key-%d", time.Now().UnixNano()%1000)
	return key
}
