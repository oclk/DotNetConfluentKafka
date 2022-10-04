using Confluent.Kafka;
using DotNetConfluentKafka.KafkaServices;
using DotNetConfluentKafka.Middlewares;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddSingleton<KafkaClientHandle>();
builder.Services.AddSingleton<WeatherForecastProducer<Null, string>>();
builder.Services.AddSingleton<WeatherForecastProducer<string, long>>();
builder.Services.AddSingleton<WeatherForecastProducer<string, string>>();
builder.Services.AddHostedService<WeatherForecastConsumer>();
builder.Services.AddControllersWithViews();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

// app.UseMiddleware<LoggingMiddleware>();
app.UseDeveloperExceptionPage();
app.UseRouting();

app.Run();
