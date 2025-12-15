# High Throughput Kafka Consumer
C# High-throughput, Simple Confluent.Kafka consumer.


This project focuses on **throughput, reliability, and simplicity** — no over-engineering, no unnecessary abstractions.

---

## ✨ Key Features

 **High throughput**
  - Processes messages concurrently with a configurable `MaxProcCount`
  - New tasks start immediately when others complete (work-conserving)

 **Safe & reliable offset management**
  - `EnableAutoCommit = false`
  - Offsets are committed **only after successful processing**
  - Batch commits instead of per-message commits

**Time-based + batch commits**
  - Commits offsets every **30 seconds**
  - Uses Kafka’s synchronous commit efficiently and sparingly

**Rebalance-safe**
  - Handles partition assignment and revocation correctly
  - Commits offsets before partitions are revoked
  - Never commits offsets for unassigned partitions

 **Low memory footprint**
  - No channels, no queues, no buffering large batches
  - Minimal allocations, minimal GC pressure

 **Graceful shutdown**
  - Stops polling
  - Waits for all in-flight tasks to finish
  - Performs a final safe commit
  - No message loss, no partial processing

 **Production ready**
  - Handles commit failures, rebalance events, and consume errors
  - Stable under heavy load
  - Kafka-thread-safe design

---

# Architecture Overview

- **Single Kafka poll loop** (Kafka thread-safe)
- **Bounded concurrency** using `SemaphoreSlim`
- **Fire-and-forget processing tasks**
- **Thread-safe offset tracking per partition**
- **Periodic batch commits**
- **Explicit rebalance handling**


---

##  Requirements

- .NET 10 (or later)
- `Confluent.Kafka`
- ASP.NET / Generic Host

---

##  Consumer Configuration (Required)

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "high-throughput-group",
    EnableAutoCommit = false,
    EnableAutoOffsetStore = false,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    MaxPollIntervalMs = 300000,
    FetchWaitMaxMs = 50,
    FetchMinBytes = 1
};
