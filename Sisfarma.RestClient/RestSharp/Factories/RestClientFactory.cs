using RSharp = RestSharp;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.RestClient.RestSharp.Extensions;
using System.Net;
using System;

namespace Sisfarma.RestClient.RestSharp.Factories
{
    public static class RestClientFactory
    {
        public static RestClientException CreateErrorException(RSharp.RestClient restClient, RSharp.IRestResponse restResponse)
            => new RestClientException(HttpStatusCode.InternalServerError,
                restResponse.ErrorMessage,
                restResponse.Request.ToRestClientRequest(restClient.BaseUrl),
                restResponse.ToRestClientRequest());

        public static RestClientException CreateErrorException(RSharp.RestClient restClient, RSharp.IRestResponse restResponse, Exception ex)
            => new RestClientException(HttpStatusCode.InternalServerError,
                ex.Message,
                restResponse.Request.ToRestClientRequest(restClient.BaseUrl),
                restResponse.ToRestClientRequest());

        public static RestClientException CreateFailedException(RSharp.RestClient restClient, RSharp.IRestResponse restResponse)
            => RestClientException.Create(restResponse.StatusCode,
                restResponse.StatusDescription,
                restResponse.Request.ToRestClientRequest(restClient.BaseUrl),
                restResponse.ToRestClientRequest());

    }
}
