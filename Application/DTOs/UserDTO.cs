using Domain.Entities;

namespace Application.DTOs
{
    public class UserDTO
    {
        public string Name { get; set; }
        public int Age { get; set; }
        public int Id { get; set; }
        public decimal Money { get; set; }
        public User ToUserEntity()
        {
            return new User
            {
                Name = this.Name,
                Age = this.Age,
                Id = this.Id,
                Money = this.Money
            };
        }
    }
}
