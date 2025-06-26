package broker

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type PartitionWriter struct {
	MessagesChan chan string
	quit         chan struct{}
	File         *os.File
	wg           sync.WaitGroup
	mu           sync.Mutex
	closed       bool
	once         sync.Once
}

func NewPartitionWriter(path string) (*PartitionWriter, error) {
	fmt.Printf("🛠️ Creating new PartitionWriter for: %s\n", path)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	pw := &PartitionWriter{
		File:         file,
		MessagesChan: make(chan string, 1000),
		quit:         make(chan struct{}),
	}

	pw.wg.Add(1)
	go pw.startWrite()

	return pw, nil
}

var TotalWritten int
var totalWrittenMu sync.Mutex

func (pw *PartitionWriter) startWrite() {
	defer pw.wg.Done() // mark goroutine finished

	for {
		select {
		case msg := <-pw.MessagesChan:
			if err := writeMessageToPartitionWithRetry(pw.File, msg, 3, 1*time.Second); err != nil {
				log.Printf("ERROR: Failed to write message to partition: %v", err)
				continue
			}
			totalWrittenMu.Lock()
			TotalWritten++
			totalWrittenMu.Unlock()

			log.Printf("✅ Message written to partition: %s", msg)
		case <-pw.quit:
			pw.File.Sync()
			pw.File.Close()
			return
		}
	}
}

func (pw *PartitionWriter) Close() {

	pw.once.Do(func() {
		pw.mu.Lock()
		defer pw.mu.Unlock()

		if pw.closed {
			return
		}
		pw.closed = true

		close(pw.quit)
		pw.wg.Wait()
	})

}

func writeMessageToPartitionWithRetry(
	file *os.File,
	message string,
	retryAttempts int,
	delayTime time.Duration,
) error {
	messageWithNewline := message + "\n"
	var lastErr error

	for attempt := 0; attempt < retryAttempts; attempt++ {
		_, writeErr := file.WriteString(messageWithNewline)
		if writeErr != nil {
			lastErr = fmt.Errorf("write attempt %d failed: %w", attempt+1, writeErr)
			log.Printf("ERROR: %v", lastErr)
			if attempt < retryAttempts-1 {
				time.Sleep(delayTime)
			}
			continue
		}

		syncErr := file.Sync()
		if syncErr != nil {
			lastErr = fmt.Errorf("sync attempt %d failed: %w", attempt+1, syncErr)
			log.Printf("ERROR: %v", lastErr)
			if attempt < retryAttempts-1 {
				time.Sleep(delayTime)
			}
			continue
		}

		return nil
	}

	return fmt.Errorf("failed to write message after %d attempts: %w", retryAttempts, lastErr)
}

func (pw *PartitionWriter) Send(msg string) error {
	pw.mu.Lock()
	defer pw.mu.Unlock()

	if pw.closed {
		return fmt.Errorf("writer is closed")
	}

	select {
	case pw.MessagesChan <- msg:
		return nil
	default:
		return fmt.Errorf("channel full, dropping message: %s", msg)
	}
}
