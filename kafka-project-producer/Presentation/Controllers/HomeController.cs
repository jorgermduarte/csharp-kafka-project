using Microsoft.AspNetCore.Mvc;

namespace kafka_project_producer.Presentation.Controllers
{
    [ApiController]
    [Route("")]
    public class HomeController : ControllerBase
    {
        private readonly ILogger<HomeController> _logger;
        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
        }

        [HttpGet(Name = "")]
        public String Get()
        {
            return "Hello World, Producer";
        }
    }
}