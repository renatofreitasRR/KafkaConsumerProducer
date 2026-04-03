using Confluent.Kafka;
using Domain.Avro;

namespace Infrastructure.Consumers.Interfaces
{
    public interface ITopicConsumer
    {
        void Close();
        void StoreOffset(ConsumeResult<string, User> consumeResult);
        void Commit(ConsumeResult<string, User> consumeResult);
        void Commit(IEnumerable<TopicPartitionOffset> offsets);
        void Commit();
        ConsumeResult<string, User> Consume(CancellationToken cancellationToken);
        ConsumeResult<string, User> Consume(TimeSpan time);
    }
}
