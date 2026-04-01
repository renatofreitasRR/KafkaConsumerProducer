using Microsoft.AspNetCore.Mvc;

namespace Application.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class HealthCheckController : ControllerBase
    {

        [HttpGet(Name = "HealthCheck")]
        public string Get()
        {
            return "OK";
        }
    }
}
