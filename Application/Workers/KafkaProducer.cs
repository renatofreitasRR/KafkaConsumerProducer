using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Domain.Repositories;
using Infrastructure.Configurations;
using System.Text.Json;

namespace Application.Workers
{
    public class KafkaProducer : BackgroundService
    {
        private IProducer<string, Domain.Avro.User> _producer;
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
                    {
                        await Task.Delay(1000, stoppingToken);
                        return;
                    }

                    //await _userRepository.DeleteAllUsers();

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

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = "http://localhost:8081"
            };

            var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

            _producer = new ProducerBuilder<string, Domain.Avro.User>(config)
                .SetValueSerializer(new AvroSerializer<Domain.Avro.User>(schemaRegistry)
                .AsSyncOverAsync())
                .Build();
        }

        private async Task Produce(CancellationToken cancellationToken)
        {
            try
            {
                using (_logger.BeginScope("Kafka App Produce Sample Data"))
                {
                    var totalMessages = _pubsubConfiguration.TotalMessages;
                    var counter = 0;

                    if (!cancellationToken.IsCancellationRequested)
                    {

                        for (int i = 0; i < totalMessages; i++)
                        {
                            var user = new Domain.Avro.User
                            {
                                Name = "Renato",
                                Age = Random.Shared.Next(18, 60),
                                Money = Random.Shared.NextDouble() * 1000,
                                Id = Guid.NewGuid()
                            };

                            var msg = new Message<string, Domain.Avro.User>
                            {
                                Key = Guid.NewGuid().ToString(),
                                Value = user
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
