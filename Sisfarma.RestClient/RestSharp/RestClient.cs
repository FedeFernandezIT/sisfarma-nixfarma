using RestSharp;
using RestSharp.Authenticators;
using Sisfarma.RestClient.RestSharp.Factories;
using System;
using System.Threading.Tasks;

using RSharp = RestSharp;

namespace Sisfarma.RestClient.RestSharp
{
    public class RestClient : BaseRestClient, IRestClient
    {
        private RSharp.RestClient _restClient;
        private RSharp.RestRequest _request;
        private readonly string _fileLogs;

        public RestClient()
        {
            _restClient = new RSharp.RestClient();
            _restClient.ClearHandlers();
            _restClient.AddHandler("application/json", new RSharp.Serialization.Json.JsonDeserializer());
            _fileLogs = System.Configuration.ConfigurationManager.AppSettings["Directory.Setup"] + @"RestClient.logs";
        }

        public RestClient(Uri baseAddress) : base(baseAddress)
        {
            _restClient = new RSharp.RestClient(baseAddress);
            _restClient.ClearHandlers();
            _restClient.AddHandler("application/json", new RSharp.Serialization.Json.JsonDeserializer());
        }

        public RestClient(Uri baseAddress, Uri resource) : base(baseAddress, resource)
        {
            _restClient = new RSharp.RestClient(baseAddress);
            _restClient.ClearHandlers();
            _restClient.AddHandler("application/json", new RSharp.Serialization.Json.JsonDeserializer());
        }

        public IRestClient BaseAddress(string url)
        {
            if (string.IsNullOrEmpty(url))
                throw new ArgumentNullException(nameof(url));

            _baseAddress = new Uri(url);
            _restClient.BaseUrl = _baseAddress;
            return this;
        }

        public IRestClient Resource(string resource)
        {
            if (string.IsNullOrEmpty(resource))
                throw new ArgumentNullException(nameof(resource));

            var resourceEscape = Uri.EscapeUriString(resource);
            _resource = new Uri(resourceEscape, UriKind.Relative);
            if (_resource.IsAbsoluteUri)
                throw new ArgumentException("No es una ruta relativa", nameof(resource));

            _request = new RSharp.RestRequest();
            _request.Resource = resourceEscape;
            return this;
        }

        public IRestClient UseAuthenticationBasic(string username, string password)
        {
            _restClient.Authenticator = new HttpBasicAuthenticator(username, password);
            return this;
        }

        public IRestClient UseAuthenticationBasic(string token)
        {
            _restClient.Authenticator = new HttpBasicAuthentication(token);
            return this;
        }

        public async Task<T> SendGetAsync<T>()
        {
            var response = await _restClient.ExecuteGetTaskAsync(_request);
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(response.Content);
        }

        public T SendGet<T>() => DoSend<T>(RSharp.Method.GET);

        public void SendGet() => DoSend(RSharp.Method.GET, null);

        public T SendPost<T>(object body) => DoSend<T>(RSharp.Method.POST, body);

        public void SendPost(object body = null) => DoSend(RSharp.Method.POST, body);

        public void SendPut(object body = null) => DoSend(RSharp.Method.PUT, body);

        public void SendDelete(object body = null) => DoSend(RSharp.Method.DELETE, body);

        public async Task<T> SendPostAsync<T>(object body)
        {
            var response = await _restClient.ExecutePostTaskAsync(_request.AddJsonBody(body));
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(response.Content);
        }

        public T SendPut<T>(object body)
        {
            var response = _restClient.Execute(_request.AddJsonBody(body), RSharp.Method.PUT);
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(response.Content);
        }

        private T DoSend<T>(RSharp.Method method)
        {
            var response = _restClient.Execute(_request, method);

            if (response.IsSuccessful)
                return Deserialize<T>(response);

            if (response.ErrorException != null)
                throw RestClientFactory.CreateErrorException(_restClient, response);

            throw RestClientFactory.CreateFailedException(_restClient, response);
        }

        private T DoSend<T>(RSharp.Method method, object body)
        {            
            _request.AddParameter(
                "application/json", 
                Newtonsoft.Json.JsonConvert.SerializeObject(body), 
                ParameterType.RequestBody);

            var response = _restClient.Execute(_request, method);

            if (response.IsSuccessful)
                return Deserialize<T>(response);

            if (response.ErrorException != null)
                throw RestClientFactory.CreateErrorException(_restClient, response);

            throw RestClientFactory.CreateFailedException(_restClient, response);
        }

        private void DoSend(RSharp.Method method, object body)
        {
            _request.AddParameter(
                "application/json",
                Newtonsoft.Json.JsonConvert.SerializeObject(body),
                ParameterType.RequestBody);

            var response = _restClient.Execute(_request, method);

            if (response.ErrorException != null)
                throw RestClientFactory.CreateErrorException(_restClient, response);

            if (!response.IsSuccessful)
                throw RestClientFactory.CreateFailedException(_restClient, response);
        }

        private T Deserialize<T>(IRestResponse response)
        {
            try
            {
                return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(response.Content);
            }
            catch (Exception ex)
            {
                throw RestClientFactory.CreateErrorException(_restClient, response, ex);
            }
        }
    }
}