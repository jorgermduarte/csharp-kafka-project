using kafka_project_producer.Infrastructure.Kafka.Producers;
using kafka_project_producer.Presentation.Models;
using Microsoft.AspNetCore.Mvc;
using System.Xml;

namespace kafka_project_producer.Presentation.Controllers
{
    [Route("api/kafka")]
    [ApiController]
    public class KafkaController : ControllerBase
    {
        private readonly KafkaProducerService _kafkaProducerService;

        public KafkaController(KafkaProducerService kafkaProducerService)
        {
            _kafkaProducerService = kafkaProducerService;
        }

        [HttpPost("test")]
        public IActionResult SendMessageToKafka([FromBody] KafkaMessageModel entity)
        {
            try
            {
                _kafkaProducerService.ProduceMessage(entity.TopicName, entity.Message);
                return Ok("Message delivered to kafka");
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Error sending the message: {ex.Message}");
            }
        }
    }
}
