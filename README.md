# Kafka-Go
# Kafka-Go 🦜⚙️  
A simplified Kafka-like messaging system implemented in **Go**. This is a learning-focused project that simulates core Kafka functionalities such as topics, partitions, producers, consumers, and file-backed storage.

---

## 🚀 Features

- ✅ Topic and Partition creation  
- ✅ Custom partitioning logic (e.g., hash-based)  
- ✅ Append-only log files per partition  
- ✅ In-memory write cache with flushing  
- ✅ Producer API to send messages  
- ✅ Consumer API to read messages sequentially  
- ✅ Basic concurrency using Goroutines and Channels  
- ✅ File persistence for fault-tolerance  
- ✅ Internal simulation of message batching and log ordering  
- 🧪 Unit tested and modular design

---

## 🧠 Project Structure
kafka-go/
├── cmd/
│ ├── broker/ # Producer CLI entrypoint
│ ├── producer/ # Producer CLI entrypoint
│ └── consumer/ # Consumer CLI entrypoint
├── internal/
│ ├── broker/ # Core message broker logic (topics, partitions)
│ ├── producer/ # Producer logic
│ ├── consumer/ # Consumer logic
│ └── utils/ # Hashing, file helpers, etc.
├── kafka-data/ # Log data stored on disk
└── README.md