using Confluent.Kafka;
using DotNetConfluentKafka.KafkaServices;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace DotNetConfluentKafka.Controllers
{
    [ApiController]
    [Route("[controller]/[action]")]
    public class WeatherForecastController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private string _topic;
        private readonly WeatherForecastProducer<string, string> _producer;
        private readonly ILogger<WeatherForecastController> _logger;

        public WeatherForecastController(WeatherForecastProducer<string, string> producer, IConfiguration config, ILogger<WeatherForecastController> logger)
        {
            _topic = config.GetValue<string>("Kafka:WeatherForecastTopic");
            _producer = producer;
            _logger = logger;
        }

        [HttpGet]
        public async Task<IActionResult> GetWeatherForecast()
        {
            List<WeatherForecast> response = Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateTime.Now.AddDays(index),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            })
            .ToList();
            string serializedList = JsonConvert.SerializeObject(response);
            await _producer.ProduceAsync(_topic, new Message<string, string> { Key = "GetWeatherForecast", Value = serializedList });
            return Ok(response);
        }
    }
}