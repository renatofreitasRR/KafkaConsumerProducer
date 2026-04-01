using Confluent.Kafka;

namespace Infrastructure.Consumers.Interfaces
{
    public interface ITopicConsumer
    {
        void Close();
        void Commit();
        void Commit(ConsumeResult<string, string> consumeResult);
        ConsumeResult<string, string> Consume(CancellationToken cancellationToken);
    }
}
