using Application.DTOs;
using Confluent.Kafka;
using Domain.Entities;
using Domain.Repositories;
using Infrastructure.Configurations;
using Infrastructure.Consumers;
using Infrastructure.Consumers.Interfaces;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;

namespace Application.Workers
{
    public class SampleTopicConsumerWorker : BackgroundService
    {
        private readonly Channel<ConsumeResult<string, string>> _channel;
        private readonly IUserRepository _userRepository;
        private readonly ITopicConsumer _consumer;
        private readonly PubSubConfiguration _pubSubConfiguration;
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
        private long _processedMessageCount = 0;
        private long _lastProcessedCount = 0;

        public SampleTopicConsumerWorker(IUserRepository userRepository, ITopicConsumer topicConsumer, PubSubConfiguration pubSubConfiguration)
        {
            _userRepository = userRepository;

            _channel = Channel.CreateBounded<ConsumeResult<string, string>>(new BoundedChannelOptions(10000)
            {
                SingleWriter = true,
                SingleReader = false,
                FullMode = BoundedChannelFullMode.Wait,
            });

            var consumer = new KafkaConsumer(new TopicConfiguration
            {
                Broker = "localhost:9092",
                ConsumerGroup = "sample-topic",
                TopicName = "sample-topic",
            });

            _pubSubConfiguration = pubSubConfiguration;
            _consumer = topicConsumer;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consumeTask = Task.Run(() => ConsumeTopic(stoppingToken));
            var printTask = Task.Run(() => PrintMetrics(stoppingToken));

            var workers = Enumerable.Range(0, 10)
            .Select(_ => Task.Run(() => ProcessMessages(stoppingToken)));

            await Task.WhenAll(workers.Append(consumeTask).Append(printTask));
        }
     
        private async Task ConsumeTopic(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                if(_pubSubConfiguration.CanConsume is false)
                {
                    await Task.Delay(100, stoppingToken);
                    continue;
                }

                var consumeResult = _consumer.Consume(stoppingToken);

                if (consumeResult.Message is not null)
                {
                    var message = consumeResult.Message.Value;

                    //Console.WriteLine($"Received message from Key: {consumeResult.Message.Key} Offset: {consumeResult.Offset}");

                    await _channel.Writer.WriteAsync(consumeResult);
                }
            }
        }

        private async Task ProcessMessages(CancellationToken stoppingToken)
        {
            var batch = new List<ConsumeResult<string, string>>();

            await foreach (var result in _channel.Reader.ReadAllAsync(stoppingToken))
            {
                //Console.WriteLine($"Processing message: {result}");

                try
                {
                    batch.Add(result);

                    if (batch.Count >= 100)
                    {
                        await ProcessBatch(batch);

                        batch.Clear();
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

        private async Task ProcessBatch(List<ConsumeResult<string, string>> batch)
        {
            var users = new List<User>();

            foreach (var result in batch)
            {
                try
                {
                    var dto = JsonSerializer.Deserialize<UserDTO>(result.Message.Value);

                    if (dto == null)
                        continue;

                    users.Add(dto.ToUserEntity());
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

            await _userRepository.InsertBatch(users);

            _consumer.Commit(batch.Last());
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
