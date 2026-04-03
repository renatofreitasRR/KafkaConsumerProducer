using Domain.Entities;

namespace Infrastructure.HttpClients
{
    public interface IUserApiClient
    {
        Task NotifyNewUser(User user);
    }
}
