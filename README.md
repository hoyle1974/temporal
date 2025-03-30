# Temporal

A simple temporal datastore used by Khronostore

A persistent, append-only, key-value store with temporal capabilities. This project provides a data structure that allows for storing and retrieving data associated with specific timestamps. Writes are strictly append-only (no rewriting history), while reads can access data at any point in time.

I wrote an original version used by my Khronostore project but decided to split it out to make it easier to develop for and more generalized. In doing this I've made it more robust, faster, and have more features.

# Project Overview
The temporal project consists of two main components:

- Storage Layer (storage.go): This layer abstracts the underlying persistent storage mechanism. It defines an interface (storage.System) that handles file-level operations: writing, reading, deleting, and listing files based on keys. This allows for easy swapping of different storage backends (e.g., local filesystem, cloud storage like S3). The interface also includes support for streaming writes.

- Temporal Map (map.go): This is the core data structure, implementing a key-value store with temporal capabilities. It leverages the storage layer for persistence and manages data across time. 

It supports:

- Append-only writes: Set and Del operations add new data or delete existing data at a given timestamp. Writes are only allowed at or after the current time.
- Temporal reads: Get and GetAll operations retrieve data at a specified point in time, reflecting the state of the data at that moment. Reads can access data from any point in the past.
- Efficient storage: Data is chunked to optimize storage and retrieval. An in-memory event sink buffers writes until a certain size is reached, then flushes them to persistent storage. This balances performance and storage efficiency.
- Metadata: The map tracks the minimum and maximum timestamps of stored data.

# Data Model
The data is stored as a series of events. Each event contains:

- A timestamp (time.Time) with nanosecond granularity
- A key (string)
- Data ([]byte)
- A boolean indicating whether it's a deletion (Delete)

Events are appended to the storage, maintaining a complete history. The temporalMap uses an index to efficiently retrieve the state at any given point in time.

Events are initially appended to an event log.  Intermittently these event logs are compacted and compressed into chunk files (~8mb each by default).  The plan is to have this optimized for S3 storage.  

# Usage

The temporalMap provides a simple interface for interacting with the temporal key-value store:

```go
// Initialize a storage system to use
myStorage := storage.NewMemoryStorage()

// Create a new temporal map (using a storage implementation)
tm, err := temporal.NewMap(myStorage)
if err != nil {
    // Handle error
}

// Set a value
err = tm.Set(context.Background(), time.Now(), "mykey", []byte("myvalue"))

// Get a value at a specific time
data, err := tm.Get(context.Background(), somePastTime, "mykey")

// Delete a value
err = tm.Del(context.Background(), time.Now(), "mykey")

// Get all values at a specific time
allData, err := tm.GetAll(context.Background(), somePastTime)
```

# Future Improvements
- Add more robust error handling.
- Implement different storage backends (e.g., S3, other cloud storage).
- Add more sophisticated indexing for faster lookups.
- Consider adding features like compaction or garbage collection for older data.
- CLI tool for working with persisted data.

This README provides a comprehensive overview of the temporal project. Further details can be found in the individual Go files.