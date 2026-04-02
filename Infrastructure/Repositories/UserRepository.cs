using Domain.Entities;
using Domain.Repositories;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Data;

namespace Infrastructure.Repositories
{
    public class UserRepository : IUserRepository
    {
        private readonly string _connectionString;

        public UserRepository(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("SqlServer");
        }

        public async Task InsertUser(User user)
        {
            // O uso do 'using' garante que a conexão seja fechada e descartada corretamente
            using (var connection = new SqlConnection(_connectionString))
            {
                const string sql = @"
                    INSERT INTO Users (Id, Name, Age, Money) 
                    VALUES (@Id, @Name, @Age, @Money);";

                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.Add("@Id", SqlDbType.Int).Value = user.Id;
                    command.Parameters.Add("@Name", SqlDbType.NVarChar).Value = user.Name;
                    command.Parameters.Add("@Age", SqlDbType.Int).Value = user.Age;
                    command.Parameters.Add("@Money", SqlDbType.Decimal).Value = user.Money;

                    await connection.OpenAsync();
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        public async Task InsertBatch(List<User> users)
        {
            if (users == null || users.Count == 0)
                return;

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = "Users",
                BatchSize = users.Count,
                BulkCopyTimeout = 0 // sem timeout
            };

            // Mapeamento de colunas
            bulkCopy.ColumnMappings.Add("Id", "Id");
            bulkCopy.ColumnMappings.Add("Name", "Name");
            bulkCopy.ColumnMappings.Add("Age", "Age");
            bulkCopy.ColumnMappings.Add("Money", "Money");

            var table = CreateDataTable(users);

            await bulkCopy.WriteToServerAsync(table);
        }

        private DataTable CreateDataTable(List<User> users)
        {
            var table = new DataTable();

            table.Columns.Add("id", typeof(int));
            table.Columns.Add("name", typeof(string));
            table.Columns.Add("age", typeof(int));
            table.Columns.Add("money", typeof(decimal));

            foreach (var user in users)
            {
                table.Rows.Add(user.Id, user.Name, user.Age, user.Money);
            }

            return table;
        }

        public async Task DeleteAllUsers()
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                const string sql = "TRUNCATE TABLE Users";

                using (var command = new SqlCommand(sql, connection))
                {
                    await connection.OpenAsync();
                    await command.ExecuteNonQueryAsync();
                }
            }
        }
    }
}