using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaWorker
{
    // Configuration Model
    public class KafkaConsumerConfig
    {
        public string BootstrapServers { get; set; } = "localhost:9092";
        public string GroupId { get; set; } = "high-throughput-group";
        public string Topic { get; set; } = "my-topic";
        public int MaxProcCount { get; set; } = 50; // Concurrency Limit
        public int MaxMsgCountBeforeCommit { get; set; } = 100; // Batch Size
        public int CommitIntervalSeconds { get; set; } = 30; // Batch Time
        public bool EnableAutoCommit { get; set; } = false; // Forced false
    }

    public class HighThroughputConsumer : BackgroundService
    {
        private readonly ILogger<HighThroughputConsumer> _logger;
        private readonly KafkaConsumerConfig _config;
        private readonly IConsumer<Ignore, string> _consumer;
        
        // Throttling: Limits concurrent Task.Run executions
        private readonly SemaphoreSlim _concurrencyLimiter;
        
        // Tracker: Tracks the state of every in-flight message per partition
        private readonly ConcurrentDictionary<TopicPartition, PartitionOffsetTracker> _offsetTrackers 
            = new ConcurrentDictionary<TopicPartition, PartitionOffsetTracker>();

        private DateTime _lastCommitTime = DateTime.UtcNow;
        private int _messagesProcessedSinceCommit = 0;

        public HighThroughputConsumer(IOptions<KafkaConsumerConfig> config, ILogger<HighThroughputConsumer> logger)
        {
            _logger = logger;
            _config = config.Value;
            _concurrencyLimiter = new SemaphoreSlim(_config.MaxProcCount);

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _config.BootstrapServers,
                GroupId = _config.GroupId,
                EnableAutoCommit = false, // Critical: We manage commits manually
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false, // We store offsets only when contiguous logic allows
                // Optimization for throughput
                FetchWaitMaxMs = 50,
                SocketReceiveBufferBytes = 1024 * 1024 
            };

            _consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
                .SetErrorHandler((_, e) => _logger.LogError($"Kafka Error: {e.Reason}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    _logger.LogInformation($"Assigned partitions: [{string.Join(", ", partitions)}]");
                    // Initialize trackers for new partitions
                    foreach (var p in partitions)
                    {
                        _offsetTrackers.TryAdd(p, new PartitionOffsetTracker());
                    }
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    _logger.LogWarning($"Revoked partitions: [{string.Join(", ", partitions)}]");
                    // Commit explicitly before losing ownership if possible
                    CommitOffsets(c); 
                    
                    // Cleanup trackers to avoid memory leaks
                    foreach (var p in partitions)
                    {
                        _offsetTrackers.TryRemove(p, out _);
                    }
                })
                .Build();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _consumer.Subscribe(_config.Topic);
            _logger.LogInformation("Consumer started. Waiting for messages...");

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    // 1. Wait for concurrency slot availability
                    // We do this BEFORE Consume to apply backpressure. 
                    // If max tasks are running, we stop pulling from Kafka.
                    await _concurrencyLimiter.WaitAsync(stoppingToken);

                    try
                    {
                        // 2. Consume with short timeout to allow Commit checks
                        var consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(100));

                        if (consumeResult == null || consumeResult.IsPartitionEOF)
                        {
                            // Slot not used, release immediately
                            _concurrencyLimiter.Release(); 
                            
                            // Check if we need to commit based on time even if idle
                            CheckAndCommit(force: false);
                            continue;
                        }

                        // 3. Register message in tracker (Order is preserved here)
                        var tracker = _offsetTrackers.GetOrAdd(consumeResult.TopicPartition, _ => new PartitionOffsetTracker());
                        tracker.AddOffset(consumeResult.Offset);

                        // 4. Offload processing to ThreadPool
                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                await ProcessMessage(consumeResult, stoppingToken);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Error processing message");
                                // Decide: Retry? DeadLetter? 
                                // For this example, we mark complete to not block the queue, 
                                // but in production, you might want a DLQ.
                            }
                            finally
                            {
                                // Mark as done in tracker
                                tracker.MarkComplete(consumeResult.Offset);
                                
                                // Release slot for new message
                                _concurrencyLimiter.Release();
                                
                                // Increment atomic counter for batch logic
                                Interlocked.Increment(ref _messagesProcessedSinceCommit);
                            }
                        }, stoppingToken);

                        // 5. Check if we need to commit
                        CheckAndCommit(force: false);
                    }
                    catch (OperationCanceledException)
                    {
                        break; // StopAsync called
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error in Consume Loop");
                        _concurrencyLimiter.Release(); // Ensure release on error
                    }
                }
            }
            finally
            {
                // StopAsync logic hits here
                await GracefulShutdown();
            }
        }

        // Simulate Business Logic
        private async Task ProcessMessage(ConsumeResult<Ignore, string> result, CancellationToken token)
        {
            // Simulate variable workload duration
            await Task.Delay(Random.Shared.Next(50, 200), token);
            // _logger.LogDebug($"Processed: {result.TopicPartitionOffset}");
        }

        private void CheckAndCommit(bool force)
        {
            var now = DateTime.UtcNow;
            var timeElapsed = (now - _lastCommitTime).TotalSeconds >= _config.CommitIntervalSeconds;
            var countReached = _messagesProcessedSinceCommit >= _config.MaxMsgCountBeforeCommit;

            if (force || timeElapsed || countReached)
            {
                CommitOffsets(_consumer);
                _lastCommitTime = DateTime.UtcNow;
                Interlocked.Exchange(ref _messagesProcessedSinceCommit, 0);
            }
        }

        private void CommitOffsets(IConsumer<Ignore, string> consumer)
        {
            var offsetsToCommit = new List<TopicPartitionOffset>();

            foreach (var kvp in _offsetTrackers)
            {
                var highestCommittable = kvp.Value.GetHighestCommittableOffset();
                if (highestCommittable != null)
                {
                    // Commit offset + 1 because Kafka commit is "next offset to fetch"
                    offsetsToCommit.Add(new TopicPartitionOffset(kvp.Key, highestCommittable.Value + 1));
                }
            }

            if (offsetsToCommit.Any())
            {
                try
                {
                    consumer.Commit(offsetsToCommit);
                    // _logger.LogInformation($"Committed {offsetsToCommit.Count} partition offsets.");
                }
                catch (KafkaException kex)
                {
                    _logger.LogError(kex, "Kafka Commit Error");
                    // In a production loop, we continue. 
                    // Next cycle will try to commit again (cumulative offsets).
                }
            }
        }

        private async Task GracefulShutdown()
        {
            _logger.LogInformation("Stopping... waiting for pending tasks.");

            // 1. Wait for active tasks to drain
            // We loop until Semaphore count equals MaxProcCount (all slots free)
            // or a timeout occurs.
            var timeout = DateTime.UtcNow.AddSeconds(10);
            while (_concurrencyLimiter.CurrentCount < _config.MaxProcCount && DateTime.UtcNow < timeout)
            {
                await Task.Delay(100);
            }

            // 2. Final Commit
            CommitOffsets(_consumer);

            _consumer.Close();
            _consumer.Dispose();
            _logger.LogInformation("Consumer Stopped cleanly.");
        }
    }

    /// <summary>
    /// Tracks offsets for a single partition to ensure we never skip offsets.
    /// Implements a "Sliding Window" of acknowledgments.
    /// Thread-Safe.
    /// </summary>
    public class PartitionOffsetTracker
    {
        private readonly object _lock = new object();
        // Stores offsets in insertion order (FIFO)
        private readonly LinkedList<long> _pendingOffsets = new LinkedList<long>();
        // Fast lookup for marking complete O(1)
        private readonly Dictionary<long, LinkedListNode<long>> _nodeLookup = new Dictionary<long, LinkedListNode<long>>();
        // Set of completed offsets (waiting for the head to catch up)
        private readonly HashSet<long> _completedOffsets = new HashSet<long>();
        
        private long? _highestCommittableOffset = null;

        public void AddOffset(long offset)
        {
            lock (_lock)
            {
                var node = _pendingOffsets.AddLast(offset);
                _nodeLookup[offset] = node;
            }
        }

        public void MarkComplete(long offset)
        {
            lock (_lock)
            {
                if (!_nodeLookup.ContainsKey(offset)) return; // Already processed or revoked

                _completedOffsets.Add(offset);
                
                // Sliding Window Logic:
                // We can only advance the committable offset if the HEAD of the list is complete.
                // If the head is complete, we remove it and check the next one.
                while (_pendingOffsets.First != null)
                {
                    var headOffset = _pendingOffsets.First.Value;
                    
                    if (_completedOffsets.Contains(headOffset))
                    {
                        // Head is done, move water mark up
                        _highestCommittableOffset = headOffset;
                        
                        // Cleanup memory
                        _completedOffsets.Remove(headOffset);
                        _nodeLookup.Remove(headOffset);
                        _pendingOffsets.RemoveFirst();
                    }
                    else
                    {
                        // Head is NOT done. We cannot commit anything beyond this point 
                        // even if later messages are done.
                        break;
                    }
                }
            }
        }

        public Offset? GetHighestCommittableOffset()
        {
            lock (_lock)
            {
                return _highestCommittableOffset.HasValue ? new Offset(_highestCommittableOffset.Value) : null;
            }
        }
    }
}
