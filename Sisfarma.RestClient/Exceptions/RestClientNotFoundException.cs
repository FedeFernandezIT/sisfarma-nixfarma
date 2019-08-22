using System.Net;

namespace Sisfarma.RestClient.Exceptions
{
    public class RestClientNotFoundException : RestClientException
    {
        public RestClientNotFoundException()
            : base(HttpStatusCode.NotFound, $"{HttpStatusCode.NotFound}")
        { }

        public RestClientNotFoundException(string statusDescription)
            : base(HttpStatusCode.NotFound, statusDescription)
        { }
    }
}
