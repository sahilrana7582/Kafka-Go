package models

import "time"

type Message struct {
	Key       string
	Topic     string
	Partition string
	Payload   string
	Timestamp time.Time
}
