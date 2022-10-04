using Confluent.Kafka;

namespace DotNetConfluentKafka.KafkaServices
{
    /// <summary>
    ///     Leverages the injected KafkaClientHandle instance to allow
    ///     Confluent.Kafka.Message{K,V}s to be produced to Kafka.
    /// </summary>
    public class WeatherForecastProducer<K, V>
    {
        IProducer<K, V> _producer;

        public WeatherForecastProducer(KafkaClientHandle handle)
        {
            _producer = new DependentProducerBuilder<K, V>(handle.Handle).Build();
        }

        /// <summary>
        ///     Asychronously produce a message and expose delivery information
        ///     via the returned Task. Use this method of producing if you would
        ///     like to await the result before flow of execution continues.
        /// <summary>
        public Task ProduceAsync(string topic, Message<K, V> message)
            => _producer.ProduceAsync(topic, message);

        /// <summary>
        ///     Asynchronously produce a message and expose delivery information
        ///     via the provided callback function. Use this method of producing
        ///     if you would like flow of execution to continue immediately, and
        ///     handle delivery information out-of-band.
        /// </summary>
        public void Produce(string topic, Message<K, V> message, Action<DeliveryReport<K, V>> deliveryHandler = null)
            => _producer.Produce(topic, message, deliveryHandler);

        public void Flush(TimeSpan timeout)
            => _producer.Flush(timeout);
    }
}