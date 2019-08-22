using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.RestClient.Exceptions
{
    public class RestClientException : Exception
    {
        public Request Request { get; private set; }

        public Response Response { get; private set; }

        public string Content { get; private set; }

        public string Verb { get; private set; }

        public string Url { get; set; }

        public string Body { get; private set; }

        public HttpStatusCode HttpStatus { get; private set; }

        public string HttpStatusDescription { get; private set; }

        public RestClientException() : this(HttpStatusCode.InternalServerError, "Internal Server Error")
        {
        }

        public RestClientException(HttpStatusCode httpStatus, string httpStatusDescription)
        {
            HttpStatus = httpStatus;
            HttpStatusDescription = httpStatusDescription ?? throw new ArgumentNullException(nameof(httpStatusDescription));
        }

        public RestClientException(HttpStatusCode httpStatus, string httpStatusDescription, Request request, Response response)
        {
            HttpStatus = httpStatus;
            HttpStatusDescription = httpStatusDescription ?? throw new ArgumentNullException(nameof(httpStatusDescription));
            Request = request ?? throw new ArgumentNullException(nameof(request));
            Response = response ?? throw new ArgumentNullException(nameof(response));
        }

        public static RestClientException Create(HttpStatusCode statusCode, string statusDescription, Request request, Response response)
        {            
            switch (statusCode)
            {
                case HttpStatusCode.NotFound:
                    return new RestClientNotFoundException(statusDescription);
                
                default:
                    return new RestClientException(statusCode, statusDescription, request, response);
            }
        }
    }
}