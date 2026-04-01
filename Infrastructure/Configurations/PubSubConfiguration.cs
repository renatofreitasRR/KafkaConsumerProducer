namespace Infrastructure.Configurations
{
    public class PubSubConfiguration
    {
        public bool CanConsume { get; set; }
        public bool CanProduce { get; set; }

        public void EnableConsumption()
        {
            CanConsume = true;
        }
    }
}
