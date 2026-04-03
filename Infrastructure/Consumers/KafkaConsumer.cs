using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Domain.Avro;
using Infrastructure.Configurations;
using Infrastructure.Consumers.Interfaces;
using Microsoft.Extensions.Logging;

namespace Infrastructure.Consumers
{
    public class KafkaConsumer : ITopicConsumer
    {
        private IConsumer<string, User> _consumer;
        private ILogger<KafkaConsumer> _logger;
        public KafkaConsumer(TopicConfiguration topicConfiguration, ILogger<KafkaConsumer> logger)
        {
            _logger = logger;

            var consumerConfig = new ConsumerConfig()
            {
                BootstrapServers = topicConfiguration.Broker,
                GroupId = topicConfiguration.ConsumerGroup,
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = false,

                //tuning pra benchmark
                //FetchMinBytes = 1024 * 1024,
                FetchWaitMaxMs = 1000,
                //QueuedMinMessages = 100000,
                //MaxPollIntervalMs = 300000
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = "localhost:8081"
            };

            var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

            _consumer = new ConsumerBuilder<string, User>(consumerConfig)
                .SetValueDeserializer(new AvroDeserializer<User>(schemaRegistry).AsSyncOverAsync())
                .Build();

            _consumer.Subscribe(new List<string>() { topicConfiguration.TopicName });
        }

        public void Close()
        {
            _consumer.Close();
        }

        public void Commit()
        {
            _consumer.Commit();

        }

        public void StoreOffset(ConsumeResult<string, User> consumeResult)
        {
            _consumer.StoreOffset(consumeResult);
        }

        public void Commit(ConsumeResult<string, User> consumeResult)
        {
            _consumer.Commit(consumeResult);
        }

        public void Commit(IEnumerable<TopicPartitionOffset> offsets)
        {
            _consumer.Commit(offsets);
        }

        public ConsumeResult<string, User> Consume(CancellationToken cancellationToken)
        {
            try
            {
                var consumeResult = _consumer.Consume(cancellationToken);
                return consumeResult;
            }
            catch (Exception ex)
            {
                _logger.LogInformation($"Error kafka: {ex.Message}");

                throw;
            }
        }

        public ConsumeResult<string, User> Consume(TimeSpan time)
        {
            try
            {
                var consumeResult = _consumer.Consume(time);
                return consumeResult;
            }
            catch (Exception ex)
            {
                _logger.LogInformation($"Error kafka: {ex.Message}");

                throw;
            }
        }
    }
}
