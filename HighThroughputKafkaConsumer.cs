public interface IKafkaMessageHandler<TKey, TValue>
{
    Task HandleAsync(ConsumeResult<TKey, TValue> message, CancellationToken ct);
}

using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading;

public sealed class HighThroughputKafkaConsumer<TKey, TValue> : BackgroundService
{
    private readonly IConsumer<TKey, TValue> _consumer;
    private readonly IKafkaMessageHandler<TKey, TValue> _handler;
    private readonly ILogger<HighThroughputKafkaConsumer<TKey, TValue>> _logger;

    private readonly SemaphoreSlim _parallelism;
    private readonly int _maxConcurrency;

    private readonly ConcurrentDictionary<TopicPartition, long> _completedOffsets = new();
    private readonly HashSet<TopicPartition> _assignedPartitions = new();

    private readonly TimeSpan _commitInterval = TimeSpan.FromSeconds(30);
    private DateTime _lastCommit = DateTime.UtcNow;

    public HighThroughputKafkaConsumer(
        ConsumerConfig config,
        string topic,
        int maxProcCount,
        IKafkaMessageHandler<TKey, TValue> handler,
        ILogger<HighThroughputKafkaConsumer<TKey, TValue>> logger)
    {
        _handler = handler;
        _logger = logger;
        _maxConcurrency = maxProcCount;
        _parallelism = new SemaphoreSlim(maxProcCount);

        _consumer = new ConsumerBuilder<TKey, TValue>(config)
            .SetPartitionsAssignedHandler(OnPartitionsAssigned)
            .SetPartitionsRevokedHandler(OnPartitionsRevoked)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka error: {Error}", e))
            .Build();

        _consumer.Subscribe(topic);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await _parallelism.WaitAsync(stoppingToken);

                ConsumeResult<TKey, TValue>? msg = null;
                try
                {
                    msg = _consumer.Consume(stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Consume error");
                    _parallelism.Release();
                    continue;
                }

                if (msg == null)
                {
                    _parallelism.Release();
                    continue;
                }

                _ = ProcessMessageAsync(msg, stoppingToken);

                TryCommitByTime();
            }
        }
        catch (OperationCanceledException)
        {
            // expected on shutdown
        }
    }

    private async Task ProcessMessageAsync(ConsumeResult<TKey, TValue> msg, CancellationToken ct)
    {
        try
        {
            await _handler.HandleAsync(msg, ct);

            // record completed offset (+1 because Kafka commit is "next offset")
            _completedOffsets.AddOrUpdate(
                msg.TopicPartition,
                msg.Offset.Value + 1,
                (_, current) => Math.Max(current, msg.Offset.Value + 1)
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Message processing failed at {TopicPartitionOffset}", msg.TopicPartitionOffset);
            // message will be reprocessed (no commit)
        }
        finally
        {
            _parallelism.Release();
        }
    }

    private void TryCommitByTime()
    {
        if (DateTime.UtcNow - _lastCommit < _commitInterval)
            return;

        CommitCompletedOffsets();
        _lastCommit = DateTime.UtcNow;
    }

    private void CommitCompletedOffsets()
    {
        if (_completedOffsets.IsEmpty)
            return;

        var offsets = _completedOffsets
            .Where(kv => _assignedPartitions.Contains(kv.Key))
            .Select(kv => new TopicPartitionOffset(kv.Key, new Offset(kv.Value)))
            .ToList();

        if (offsets.Count == 0)
            return;

        try
        {
            _consumer.Commit(offsets);
        }
        catch (KafkaException ex)
        {
            _logger.LogError(ex, "Commit failed");
        }
    }

    private void OnPartitionsAssigned(IConsumer<TKey, TValue> consumer, List<TopicPartition> partitions)
    {
        _assignedPartitions.Clear();
        foreach (var p in partitions)
            _assignedPartitions.Add(p);

        _logger.LogInformation("Partitions assigned: {Partitions}", string.Join(",", partitions));
    }

    private void OnPartitionsRevoked(IConsumer<TKey, TValue> consumer, List<TopicPartitionOffset> partitions)
    {
        _logger.LogInformation("Partitions revoked, committing offsets");

        CommitCompletedOffsets();

        foreach (var p in partitions)
            _assignedPartitions.Remove(p.TopicPartition);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping Kafka consumer...");

        // stop polling
        _consumer.Unsubscribe();

        // wait for in-flight tasks
        for (int i = 0; i < _maxConcurrency; i++)
            await _parallelism.WaitAsync(cancellationToken);

        // final commit
        CommitCompletedOffsets();

        _consumer.Close();
        _consumer.Dispose();

        await base.StopAsync(cancellationToken);
    }
}
