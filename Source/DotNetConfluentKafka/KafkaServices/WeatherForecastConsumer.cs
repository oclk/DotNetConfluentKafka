using Confluent.Kafka;
using Newtonsoft.Json;

namespace DotNetConfluentKafka.KafkaServices
{
    /// <summary>
    ///     A simple example demonstrating how to set up a Kafka consumer as an
    ///     IHostedService.
    /// </summary>
    public class WeatherForecastConsumer : BackgroundService
    {
        private readonly string _topic;
        private readonly IConsumer<string, string> _consumer;

        public WeatherForecastConsumer(IConfiguration config)
        {
            var consumerConfig = new ConsumerConfig();
            config.GetSection("Kafka:ConsumerSettings").Bind(consumerConfig);
            _topic = config.GetValue<string>("Kafka:WeatherForecastTopic");
            _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            new Thread(() => _ = ConsumerLoop(stoppingToken)).Start();
            return Task.CompletedTask;
        }

        private async Task ConsumerLoop(CancellationToken cancellationToken)
        {
            _consumer.Subscribe(_topic);

            await Task.Yield(); // Added to solve main thread lock
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var cr = _consumer.Consume(cancellationToken);

                    // Handle message...
                    Console.WriteLine($"{cr.Message.Key}: {cr.Message.Value}ms");
                    List<WeatherForecast>? deserilizedResult = JsonConvert.DeserializeObject<List<WeatherForecast>>(cr.Message.Value);
                }
                catch (OperationCanceledException e)
                {
                    Console.WriteLine($"Consume error: {e.Message}");
                    break;
                }
                catch (ConsumeException e)
                {
                    // Consumer errors should generally be ignored (or logged) unless fatal.
                    Console.WriteLine($"Consume error: {e.Error.Reason}");

                    if (e.Error.IsFatal)
                    {
                        // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                        break;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Unexpected error: {e}");
                    break;
                }
            }
        }

        public override void Dispose()
        {
            _consumer.Close(); // Commit offsets and leave the group cleanly.
            _consumer.Dispose();

            base.Dispose();
        }
    }
}