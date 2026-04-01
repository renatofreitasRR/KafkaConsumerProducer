using Domain.Entities;

namespace Domain.Repositories
{
    public interface IUserRepository
    {
        Task DeleteAllUsers();
        Task InsertUser(User user);
        Task InsertBatch(List<User> users);
    }
}
