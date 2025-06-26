package broker

import (
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
