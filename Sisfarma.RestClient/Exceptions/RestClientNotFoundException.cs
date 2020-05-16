using System.Net;

namespace Sisfarma.RestClient.Exceptions
{
    public class RestClientNotFoundException : RestClientException
    {
        public RestClientNotFoundException(Request request, Response response)
            : base(HttpStatusCode.NotFound, $"{HttpStatusCode.NotFound}", request, response)
        { }

        public RestClientNotFoundException(string statusDescription, Request request, Response response)
            : base(HttpStatusCode.NotFound, statusDescription, request, response)
        { }
    }
}
