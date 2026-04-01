namespace Infrastructure.Configurations
{
    public class TopicConfiguration
    {
        public string Broker { get; set; }
        public string ConsumerGroup { get; set; }
        public string TopicName { get; set; }
    }
}
