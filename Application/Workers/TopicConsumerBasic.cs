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
            var timeFlush = Stopwatch.StartNew();
            var timeTotalBatch = Stopwatch.StartNew();
            var maxWaitTime = TimeSpan.FromSeconds(3);

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

                    if (userMessage == null)
                    {
                        if (
                            (timeFlush.Elapsed > maxWaitTime) &&
                            (batchMessages.Any() && users.Any())
                            )
                        {
                            await Flush(batchMessages, users);
                            timeFlush.Restart();
                        }

                        continue;
                    }

                    users.Add(new User
                    {
                        Name = userMessage.Value.Name,
                        Age = userMessage.Value.Age,
                        Id = userMessage.Value.Id,
                        Money = userMessage.Value.Money
                    });

                    batchMessages.Add(consumeResult);

                    if (users.Count == BATCH_SIZE)
                    {
                        await Flush(batchMessages, users);

                        Console.WriteLine($"Batch of {BATCH_SIZE} messages processed in {timeTotalBatch.Elapsed.TotalSeconds} seconds.");
                        timeTotalBatch.Restart();
                    }

                    timeFlush.Restart();

                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing message: {ex.Message} key: {consumeResult.Message.Key} offset: {consumeResult.Offset}");
                    continue;
                }
            }
        }


        private async Task Flush(List<ConsumeResult<string, Domain.Avro.User>> batchMessages, List<User> users)
        {
            await _userRepository.InsertBatch(users);

            _consumer.Commit(batchMessages.Last());

            batchMessages.Clear();
            users.Clear();
        }
    }
}
