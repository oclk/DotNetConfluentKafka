using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System.Diagnostics;

namespace DotNetConfluentKafka.KafkaServices
{
    /// <summary>
    ///     Wraps a Confluent.Kafka.IProducer instance, and allows for basic
    ///     configuration of this via IConfiguration.
    ///    
    ///     KafkaClientHandle does not provide any way for messages to be produced
    ///     directly. Instead, it is a dependency of KafkaDependentProducer. You
    ///     can create more than one instance of KafkaDependentProducer (with
    ///     possibly differing K and V generic types) that leverages the same
    ///     underlying producer instance exposed by the Handle property of this
    ///     class. This is more efficient than creating separate
    ///     Confluent.Kafka.IProducer instances for each Message type you wish to
    ///     produce.
    /// </summary>
    public class KafkaClientHandle : IDisposable
    {
        private readonly IProducer<byte[], byte[]> _producer;
        private readonly string _topic;

        public KafkaClientHandle(IConfiguration config)
        {
            _topic = config.GetValue<string>("Kafka:WeatherForecastTopic");
            var conf = new ProducerConfig();
            config.GetSection("Kafka:ProducerSettings").Bind(conf);
            _producer = new ProducerBuilder<byte[], byte[]>(conf).Build();

            CreateTopicAsync(conf).Wait();
        }

        public Handle Handle { get => _producer.Handle; }

        private async Task CreateTopicAsync(ClientConfig clientConfig)
        {
            using (var adminClient = new AdminClientBuilder(clientConfig).Build())
            {
                try
                {
                    var isTopicExist = adminClient.GetMetadata(TimeSpan.FromSeconds(10)).Topics.Any(t => t.Topic == _topic);
                    if (isTopicExist) return;
                    await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                        new TopicSpecification { Name = _topic, ReplicationFactor = 3, NumPartitions = 5 } });
                }
                catch (CreateTopicsException exp)
                {
                    Debug.WriteLine($"An error occured creating topic {exp.Results[0].Topic}: {exp.Results[0].Error.Reason}");
                    throw exp;
                }
            }
        }

        public void Dispose()
        {
            // Block until all outstanding produce requests have completed (with or without error).
            _producer.Flush();
            _producer.Dispose();
        }
    }
}