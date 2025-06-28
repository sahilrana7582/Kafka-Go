package producer

import (
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
	RecordCount int32 // Number of Records in this batch

	// --- Records within the batch ---
	Records []Record
}

// Represents the data structure for a single partition within a ProduceRequest.
type TopicPartitionData struct {
	PartitionID   int32
	mu            sync.Mutex
	RecordBatch   RecordBatch
	LastFlushTime time.Time
}

// Represents the data structure for a single topic within a ProduceRequest.
type TopicData struct {
	TopicName  string
	Partitions map[int32]*TopicPartitionData
}

// Represents a Kafka Produce Request.
// This is the actual network request sent from producer to broker.
type ProduceRequest struct {

	// --- Request Body ---
	Acks      int16 // 0, 1, or -1 (all)
	TimeoutMs int32 // Timeout for the request

	Topics []*TopicData
}

type Producer struct {
	Name     string
	TopicMap map[string]*TopicData
	mu       sync.RWMutex
	Broker   BrokerInterface
}

type ProducerRecord struct {
	Topic     string
	Partition *int32
	Key       string
	Value     string
}

func NewProducer(name string, broker BrokerInterface) *Producer {
	return &Producer{
		Name:     name,
		TopicMap: make(map[string]*TopicData),
		Broker:   broker,
	}
}

type BrokerInterface interface {
	ReceiveProduceRequest(req ProduceRequest)
	DescribeTopic(topicName string) (*TopicData, error)
}
