using Confluent.Kafka;
using Domain.Entities;
using Domain.Repositories;
using Infrastructure.Configurations;
using Infrastructure.Consumers;
using Infrastructure.Consumers.Interfaces;
using System.Diagnostics;
using System.Threading.Channels;

namespace Application.Workers
{
    public class SampleTopicConsumerWorker : BackgroundService
    {
        private readonly Channel<ConsumeResult<string, Domain.Avro.User>> _channel;
        private readonly IUserRepository _userRepository;
        private readonly ITopicConsumer _consumer;
        private readonly PubSubConfiguration _pubSubConfiguration;
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
        private long _processedMessageCount = 0;
        private long _lastProcessedCount = 0;

        public SampleTopicConsumerWorker(IUserRepository userRepository, ITopicConsumer topicConsumer, PubSubConfiguration pubSubConfiguration)
        {
            _userRepository = userRepository;

            _channel = Channel.CreateBounded<ConsumeResult<string, Domain.Avro.User>>(new BoundedChannelOptions(10000)
            {
                SingleWriter = true,
                SingleReader = false,
                FullMode = BoundedChannelFullMode.Wait,
            });

            _pubSubConfiguration = pubSubConfiguration;
            _consumer = topicConsumer;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {

            var workers = Enumerable.Range(0, 10)
            .Select(_ => Task.Run(() => ProcessMessages(stoppingToken)));

            var consumeTask = Task.Run(() => ConsumeTopic(stoppingToken));
            //var processTask = Task.Run(() => ProcessMessages(stoppingToken));
            var metricsTaks = Task.Run(() => PrintMetrics(stoppingToken));

            await Task.WhenAll(workers.Append(consumeTask).Append(metricsTaks));
        }

        private async Task ConsumeTopic(CancellationToken stoppingToken)
        {
            while (true)
            {
                if (_pubSubConfiguration.CanConsume is false)
                {
                    await Task.Delay(100, stoppingToken);
                    continue;
                }

                var consumeResult = _consumer.Consume(stoppingToken);

                if (consumeResult.Message is not null)
                {
                    await _channel.Writer.WriteAsync(consumeResult);
                }
            }
        }

        private async Task ProcessMessages(CancellationToken stoppingToken)
        {
            Console.WriteLine("Processando mensagens");

            var batch = new List<ConsumeResult<string, Domain.Avro.User>>();
            var batchMaxSize = 100;
            var maxWaitTime = TimeSpan.FromSeconds(1);

            var lastFlush = Stopwatch.StartNew();

            await foreach (var result in _channel.Reader.ReadAllAsync(stoppingToken))
            {
                try
                {
                    batch.Add(result);

                    var shouldFlushBySize = batch.Count >= batchMaxSize;
                    var shouldFlushByTime = lastFlush.Elapsed >= maxWaitTime;

                    if (shouldFlushBySize || shouldFlushByTime)
                    {
                        await ProcessBatch(batch);

                        batch.Clear();
                        lastFlush.Restart();
                    }

                    Interlocked.Increment(ref _processedMessageCount);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing message: {ex.Message} key: {result.Message.Key} offset: {result.Offset}");
                    continue;
                }
            }
        }

        private async Task ProcessBatch(List<ConsumeResult<string, Domain.Avro.User>> batch)
        {
            var users = new List<User>();

            foreach (var result in batch)
            {
                try
                {
                    var dto = result.Message.Value;

                    if (dto == null)
                        continue;

                    users.Add(new User
                    {
                        Name = dto.Name,
                        Age = dto.Age,
                        Id = dto.Id,
                        Money = dto.Money
                    });
                }
                catch (Exception ex)
                {
                    Console.WriteLine(
                        $"Erro ao deserializar | Offset: {result.Offset} | Key: {result.Message.Key}"
                    );
                }
            }

            if (users.Count == 0)
                return;

            try
            {
                await _userRepository.InsertBatch(users);
                _consumer.Commit(batch.Last());
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting batch: {ex.Message}");
            }
        }

        private async Task PrintMetrics(CancellationToken token)
        {
            var sw = Stopwatch.StartNew();

            while (!token.IsCancellationRequested)
            {
                await Task.Delay(3000, token); // Intervalo de 5 segundos

                long currentTotal = Interlocked.Read(ref _processedMessageCount);
                double elapsedSeconds = sw.Elapsed.TotalSeconds;

                // Mensagens processadas APENAS neste intervalo
                long messagesInInterval = currentTotal - _lastProcessedCount;
                double instantThroughput = messagesInInterval / elapsedSeconds;

                // Média Global (o que você já tinha)
                double globalThroughput = currentTotal / _stopwatch.Elapsed.TotalSeconds;

                Console.WriteLine(
                    $"Instante: {instantThroughput:F2} msg/s | " +
                    $"Média Global: {globalThroughput:F2} msg/s | " +
                    $"Total: {currentTotal}"
                );

                // Reset para o próximo intervalo
                _lastProcessedCount = currentTotal;
                sw.Restart();
            }
        }
    }
}
