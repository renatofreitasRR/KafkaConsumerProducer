using Confluent.Kafka;
using Domain.Entities;
using Domain.Repositories;
using Infrastructure.Configurations;
using Infrastructure.Consumers.Interfaces;
using System.Diagnostics;

namespace Application.Workers
{
    public class TopicConsumerBasic : BackgroundService
    {
        private readonly IUserRepository _userRepository;
        private readonly ITopicConsumer _consumer;
        private readonly PubSubConfiguration _pubSubConfiguration;
        private const int BATCH_SIZE = 1000;
        private long _counter = 0;

        public TopicConsumerBasic(IUserRepository userRepository, ITopicConsumer topicConsumer, PubSubConfiguration pubSubConfiguration)
        {
            _userRepository = userRepository;

            _pubSubConfiguration = pubSubConfiguration;
            _consumer = topicConsumer;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var batchMessages = new List<ConsumeResult<string, Domain.Avro.User>>();
            var users = new List<User>();

            var timerPerBatch = Stopwatch.StartNew();
            var timeEndToEnd = Stopwatch.StartNew();

            while (!stoppingToken.IsCancellationRequested)
            {
                if (_pubSubConfiguration.CanConsume is false)
                {
                    await Task.Delay(100, stoppingToken);
                    continue;
                }

                var consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(100));

                try
                {
                    var userMessage = consumeResult?.Message;

                    if (userMessage != null)
                    {
                        users.Add(new User
                        {
                            Name = userMessage.Value.Name,
                            Age = userMessage.Value.Age,
                            Id = userMessage.Value.Id,
                            Money = userMessage.Value.Money
                        });

                        batchMessages.Add(consumeResult);
                    }

                    if (users.Count >= BATCH_SIZE || (userMessage is null && batchMessages.Any()))
                    {
                        await Flush(batchMessages, users);
                        Console.WriteLine($"{_counter} Mensagens processadas em {timerPerBatch.Elapsed.TotalSeconds}s ");
                        timerPerBatch.Restart();
                    }

                    if (_counter == _pubSubConfiguration.TotalMessages)
                    {
                        timeEndToEnd.Stop();
                        Console.WriteLine($"Todas as mensagens foram consumidas consumidas em {timeEndToEnd.Elapsed.TotalSeconds} segundos");
                    }

                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing message: {ex.Message} key: {consumeResult?.Message?.Key} offset: {consumeResult?.Offset}");
                    continue;
                }
            }
        }


        private async Task Flush(List<ConsumeResult<string, Domain.Avro.User>> batchMessages, List<User> users)
        {
            try
            {
                await _userRepository.InsertBatch(users);

                var offsets = batchMessages
                 .GroupBy(x => x.TopicPartition)
                 .Select(g => new TopicPartitionOffset(
                     g.Key,
                     g.Max(x => x.Offset.Value) + 1
                 ));

                _consumer.Commit(offsets);
                _counter += users.Count;

                batchMessages.Clear();
                users.Clear();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting batch: {ex.Message} Ids de {users.Min(x => x.Id)} a {users.Max(x => x.Id)} Offsets de {batchMessages.Min(x => x.Offset.Value)} a {batchMessages.Max(x => x.Offset.Value)}");
                return;
            }

        }
    }
}
