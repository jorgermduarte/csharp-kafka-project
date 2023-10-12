using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace kafka_project_producer.Infrastructure.Kafka.Producers
{
    public class KafkaProducerService
    {
        private readonly ProducerConfig _producerConfig;
        private readonly IProducer<Null, string> _producer;
        private readonly AdminClientConfig _adminClientConfig;

        public KafkaProducerService()
        {
            _producerConfig = new ProducerConfig
            {
                BootstrapServers = "kafka-broker:9092",
            };
            
            _adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = _producerConfig.BootstrapServers
            };

            _producer = new ProducerBuilder<Null, string>(_producerConfig).Build();
        }

        public void ProduceMessage(string topicName, string message)
        {
            EnsureTopicExists(topicName);

            try
            {
                var deliveryReport = _producer.ProduceAsync(topicName, new Message<Null, string> { Value = message }).Result;
                Console.WriteLine($"Message divered to the topic {deliveryReport.Topic} - Partition: {deliveryReport.Partition} - Offset: {deliveryReport.Offset}");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Error producing the message: {e.Error.Reason}");
            }
        }

        private void EnsureTopicExists(string topicName)
        {
            using var adminClient = new AdminClientBuilder(_adminClientConfig).Build();
            var topicExists = adminClient.GetMetadata(TimeSpan.FromSeconds(10)).Topics.Any(topic => topic.Topic == topicName);

            if (!topicExists)
            {
                adminClient.CreateTopicsAsync(new TopicSpecification[] {
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = 1,
                    ReplicationFactor = -1
                }
            }).Wait();

                Console.WriteLine($"Topic '{topicName}' created successfully.");
            }
        }
    }
}
