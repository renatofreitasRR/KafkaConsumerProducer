using Domain.Entities;

namespace Infrastructure.HttpClients
{
    public class FakeUserAPIClient : IUserApiClient
    {
        public async Task NotifyNewUser(User user)
        {
            await Task.Delay(50);
        }
    }
}
