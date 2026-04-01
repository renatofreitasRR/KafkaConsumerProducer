using Confluent.Kafka;
using Infrastructure.Configurations;
using Infrastructure.Consumers.Interfaces;
using Microsoft.Extensions.Logging;

namespace Infrastructure.Consumers
{
    public class KafkaConsumer : ITopicConsumer
    {
        private IConsumer<string, string> _consumer;
        private ILogger<KafkaConsumer> _logger;
        public KafkaConsumer(TopicConfiguration topicConfiguration)
        {
            var consumerConfig = new ConsumerConfig()
            {
                BootstrapServers = topicConfiguration.Broker,
                GroupId = topicConfiguration.ConsumerGroup,
                EnableAutoCommit = true,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = false,

                //tuning pra benchmark
                FetchMinBytes = 1024 * 1024,
                FetchWaitMaxMs = 100,
                QueuedMinMessages = 100000,
                MaxPollIntervalMs = 300000
            };

            _consumer = new ConsumerBuilder<string, string>(consumerConfig)
                .SetKeyDeserializer(Deserializers.Utf8)
                .SetValueDeserializer(Deserializers.Utf8)
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

        public void Commit(ConsumeResult<string, string> consumeResult)
        {
            _consumer.Commit(consumeResult);
        }

        public ConsumeResult<string, string> Consume(CancellationToken cancellationToken)
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
    }
}
