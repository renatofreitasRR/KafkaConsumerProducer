using Confluent.Kafka;
using Domain.Repositories;
using Infrastructure.Configurations;
using System.Text.Json;

namespace Application.Workers
{
    public class KafkaProducer : BackgroundService
    {
        private IProducer<string, string> _producer;
        private readonly ILogger<KafkaProducer> _logger;
        private readonly PubSubConfiguration _pubsubConfiguration;
        private readonly TopicConfiguration _topicConfiguration;
        private readonly IUserRepository _userRepository;

        public KafkaProducer(
            ILogger<KafkaProducer> logger,
            PubSubConfiguration pubSubConfiguration,
            TopicConfiguration topicConfiguration,
            IUserRepository userRepository
            )
        {
            _logger = logger ?? throw new ArgumentException(nameof(logger));
            _pubsubConfiguration = pubSubConfiguration ?? throw new ArgumentException(nameof(pubSubConfiguration));
            _userRepository = userRepository;
            _topicConfiguration = topicConfiguration;

            Init();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation("Kafka Producer Service has started.");

                    if (!_pubsubConfiguration.CanProduce)
                        Task.Delay(1000, stoppingToken).Wait(stoppingToken);

                    await _userRepository.DeleteAllUsers();

                    await Produce(stoppingToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
            }
        }

        private void Init()
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = _topicConfiguration.Broker,
                LingerMs = 5,
                BatchSize = 32 * 1024,
                CompressionType = CompressionType.Snappy
            };

            _producer = new ProducerBuilder<string, string>(config)
                .SetKeySerializer(Serializers.Utf8)
                .Build();
        }

        private async Task Produce(CancellationToken cancellationToken)
        {
            try
            {
                using (_logger.BeginScope("Kafka App Produce Sample Data"))
                {
                    var totalMessages = 10000000;
                    var counter = 0;

                    if (!cancellationToken.IsCancellationRequested)
                    {

                        for (int i = 0; i < totalMessages; i++)
                        {
                            var person = new
                            {
                                Name = "Renato",
                                Age = new Random().Next(18, 60),
                                Money = new Random().NextDouble() * 1000,
                                Id = i
                            };

                            var json = JsonSerializer.Serialize(person);

                            var msg = new Message<string, string>
                            {
                                Key = Guid.NewGuid().ToString(),
                                Value = json
                            };

                            _producer.Produce(_topicConfiguration.TopicName, msg);

                            counter++;
                        }

                        if (totalMessages == counter)
                        {
                            _pubsubConfiguration.EnableConsumption();

                            _logger.LogInformation($"Produced {counter} messages to topic");
                        }
                    }

                    _producer.Flush(TimeSpan.FromSeconds(10));
                }
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, exception.Message);
            }
        }
    }
}
