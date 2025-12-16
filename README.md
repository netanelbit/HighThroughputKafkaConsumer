# High Throughput Kafka Consumer
C# High-throughput, Simple Confluent.Kafka consumer.


This project focuses on **throughput, reliability, and simplicity** — no over-engineering, no unnecessary abstractions.

---

## Key Features

Strict Ordering ("No Skipping Offsets"):
• (Conceptual Trigger)
• The PartitionOffsetTracker is the core safety mechanism.
• Example: Offsets 10, 11, 12 arrive. 12 finishes first. The tracker marks 12 as complete but keeps it in memory. 11 finishes. Still held. Finally, 10 finishes. The tracker loop sees 10 is done \rightarrow updates commit to 10. Sees 11 is done \rightarrow updates to 11. Sees 12 is done \rightarrow updates to 12.
• This guarantees that if we crash, we resume from the earliest unfinished message.
2. Concurrency Control:
• SemaphoreSlim _concurrencyLimiter: This is superior to Parallel.ForEach for infinite streams. It pauses the Consume loop when MaxProcCount tasks are active (backpressure), preventing memory explosions.
3. Low Memory Footprint:
• We do not store the full Message object in the tracker, only the long offset.
• We aggressively remove items from the LinkedList and Dictionary as soon as the "watermark" passes them.
4. Batch Committing:
• Commits happen in the main loop (CheckAndCommit) to ensure thread safety of the _consumer object.
• It triggers only after MaxMsgCount or CommitInterval is reached.
5. Fault Tolerance:
• Rebalancing: SetPartitionsRevokedHandler cleans up trackers for partitions we lost ownership of so we don't try to commit invalid offsets later.
• Graceful Shutdown: The StopAsync waits for the semaphore count to return to max (meaning all workers returned) and performs one final commit.
6. Performance:
• Using Task.Run allows CPU-bound or I/O-bound work to run without blocking the Kafka polling loop.
• EnableAutoOffsetStore = false and EnableAutoCommit = false ensures zero overhead from the library trying to manage state we are managing ourselves.

---

##  Requirements

- .NET 8 (or later)
- `Confluent.Kafka`
- ASP.NET / Generic Host

---

##  Usage

```csharp
builder.Services.Configure<KafkaConsumerConfig>(builder.Configuration.GetSection("Kafka"));
builder.Services.AddHostedService<HighThroughputConsumer>();
