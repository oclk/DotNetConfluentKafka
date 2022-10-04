using Microsoft.AspNetCore.Mvc;

namespace DotNetConfluentKafka.Controllers
{
    public class HomeController : Controller
    {
        public IActionResult Index()
        {
            return View();
        }
    }
}
