using Confluent.Kafka;

namespace kafka_project.Infrastructure.Kafka.Consumers
{
    public class KafkaBackgroundConsumer : BackgroundService
    {
        private readonly ConsumerConfig _consumerConfig;
        private readonly IConsumer<Ignore, string> _consumer;

        public KafkaBackgroundConsumer()
        {
            _consumerConfig = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "kafka-broker:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };

            _consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build();
            _consumer.Subscribe("tasks-topic"); // Subscribe to the Kafka topic
        }


        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var message = _consumer.Consume(stoppingToken);
                    Console.WriteLine($"Message read: {message.Value}");

                    // Implement the logic to process the message here
                }
                catch (OperationCanceledException)
                {
                    // The execution was cancelled
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occured: {e.Error.Reason}");
                }
            }
        }

        public override void Dispose()
        {
            _consumer.Close();
            base.Dispose();
        }
    }
}
