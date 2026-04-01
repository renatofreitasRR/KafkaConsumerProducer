using Application.Workers;
using Domain.Repositories;
using Infrastructure.Configurations;
using Infrastructure.Consumers;
using Infrastructure.Consumers.Interfaces;
using Infrastructure.Repositories;

var builder = WebApplication.CreateBuilder(args);


builder.Services.AddControllers();
builder.Services.AddOpenApi();

builder.Services.AddTransient<IUserRepository, UserRepository>();

builder.Services.AddTransient<ITopicConsumer, KafkaConsumer>();

builder.Services.AddSingleton<PubSubConfiguration>(new PubSubConfiguration
{
    CanProduce = true
});

builder.Services.AddSingleton<TopicConfiguration>(new TopicConfiguration
{
    Broker = "localhost:9092",
    TopicName = "sample-topic",
    ConsumerGroup = "sample-topic"
});


builder.Services.AddHostedService<KafkaProducer>();
builder.Services.AddHostedService<SampleTopicConsumerWorker>();


var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
