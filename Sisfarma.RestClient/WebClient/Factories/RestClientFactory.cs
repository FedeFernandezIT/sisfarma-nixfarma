using RSharp = RestSharp;
using Sisfarma.RestClient.Exceptions;
using System.Net;
using System;
using System.Net.Http;
using Sisfarma.RestClient.WebClient.Extensions;

namespace Sisfarma.RestClient.WebClient.Factories
{
    public static class RestClientFactory
    {
        public static RestClientException CreateErrorException(HttpClient restClient, HttpResponseMessage restResponse)
        {
            var errorMessage = restResponse.Content.ReadAsStringAsync().Result;

            return new RestClientException(restResponse.StatusCode,
                errorMessage,
                restResponse.RequestMessage.ToRestClientRequest(restClient.BaseAddress),
                restResponse.ToRestClientRequest());
        }

        public static RestClientException CreateErrorException(HttpClient restClient, HttpResponseMessage restResponse, Exception ex)
            => new RestClientException(HttpStatusCode.InternalServerError,
                ex.Message,
                restResponse.RequestMessage.ToRestClientRequest(restClient.BaseAddress),
                restResponse.ToRestClientRequest());

        public static RestClientException CreateFailedException(HttpClient restClient, HttpResponseMessage restResponse)
            => RestClientException.Create(restResponse.StatusCode,
                restResponse.ReasonPhrase,
                restResponse.RequestMessage.ToRestClientRequest(restClient.BaseAddress),
                restResponse.ToRestClientRequest());

        public static Exception CreateFailedException(HttpClient restClient, HttpRequestMessage requestMessage, HttpRequestException ex)
        {
            return RestClientException.Create(HttpStatusCode.InternalServerError,
                ex.Message,
                requestMessage.ToRestClientRequest(restClient.BaseAddress),
                new Response(string.Empty, 0));
        }
    }
}