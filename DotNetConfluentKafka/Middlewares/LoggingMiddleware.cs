using Confluent.Kafka;
using DotNetConfluentKafka.KafkaServices;
using System.Diagnostics;

namespace DotNetConfluentKafka.Middlewares
{
    /// <summary>
    ///     Middleware that times how long a web request takes to be handled,
    ///     and logs this to Kafka.
    /// </summary>
    public class LoggingMiddleware
    {
        private readonly string _topic;
        private readonly WeatherForecastProducer<string, long> _producer;
        private readonly RequestDelegate _next;
        private readonly ILogger _logger;

        public LoggingMiddleware(RequestDelegate next, WeatherForecastProducer<string, long> producer, IConfiguration config, ILogger<LoggingMiddleware> logger)
        {
            _next = next;
            _producer = producer;
            _topic = config.GetValue<string>("Kafka:LoggingTopic");
            _logger = logger;
        }

        public async Task Invoke(HttpContext context)
        {
            Stopwatch s = new ();
            try
            {
                s.Start();
                await _next(context);
            }
            finally
            {
                s.Stop();

                // Write request timing infor to Kafka (non-blocking), handling any errors out-of-band.
                _producer.Produce(_topic, new Message<string, long> { Key = context.Request.Path.Value, Value = s.ElapsedMilliseconds }, DeliveryReportHandler);

                // Alternatively, you can await the produce call. This will delay the request until the result of
                // the produce call is known. An exception will be throw in the event of an error.
                // await producer.ProduceAsync(_topic, new Message<string, long> { Key = context.Request.Path.Value, Value = s.ElapsedMilliseconds });
            }
        }

        private void DeliveryReportHandler(DeliveryReport<string, long> deliveryReport)
        {
            if (deliveryReport.Status == PersistenceStatus.NotPersisted)
            {
                _logger.Log(LogLevel.Warning, $"Failed to log request time for path: {deliveryReport.Message.Key}");
            }
        }
    }
}