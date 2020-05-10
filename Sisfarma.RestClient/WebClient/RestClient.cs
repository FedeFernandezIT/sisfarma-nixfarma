using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Sisfarma.RestClient.WebClient.Extensions;
using Sisfarma.RestClient.WebClient.Factories;

namespace Sisfarma.RestClient.WebClient
{
    public class RestClient : BaseRestClient, IRestClient
    {
        private HttpClient _restClient;
        private HttpRequestMessage _request;
        private readonly string FILE_LOG;
        private string _host;

        public RestClient()
        {
            _restClient = new HttpClient();
            FILE_LOG = System.Configuration.ConfigurationManager.AppSettings["Directory.Logs"] + @"RestClient.logs";
        }

        public IRestClient BaseAddress(string url)
        {
            if (string.IsNullOrEmpty(url))
                throw new ArgumentNullException(nameof(url));

            _host = url.TrimEnd('/');

            _baseAddress = new Uri(url);
            _restClient.BaseAddress = _baseAddress;
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

            var uriResource = _host + "/" + resourceEscape.TrimStart('/');
            _request = new HttpRequestMessage
            {
                RequestUri = new Uri(uriResource)
            };

            return this;
        }

        public void SendDelete(object body = null) => DoSend(HttpMethod.Delete, body);

        public T SendGet<T>() => DoSend<T>(HttpMethod.Get);

        public void SendGet() => DoSend(HttpMethod.Get, null);

        public async Task<T> SendGetAsync<T>()
        {
            _request.Method = HttpMethod.Get;

            Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " " + _request.ToRestClientRequest(_restClient.BaseAddress).ToString(), FILE_LOG);
            var response = await _restClient.SendAsync(_request);
            Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " " + response.ToRestClientRequest().ToString(), FILE_LOG);

            var json = await response.Content.ReadAsStringAsync();
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json);
        }

        public T SendPost<T>(object body) => DoSend<T>(HttpMethod.Post, body);

        public void SendPost(object body = null) => DoSend(HttpMethod.Post, body);

        public async Task<T> SendPostAsync<T>(object body)
        {
            _request.Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(body), Encoding.UTF8, "application/json");
            _request.Method = HttpMethod.Post;

            Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " " + _request.ToRestClientRequest(_restClient.BaseAddress).ToString(), FILE_LOG);
            var response = await _restClient.SendAsync(_request);
            Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " " + response.ToRestClientRequest().ToString(), FILE_LOG);

            var json = await response.Content.ReadAsStringAsync();
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json);
        }

        public void SendPut(object body = null) => DoSend(HttpMethod.Put, body);

        public T SendPut<T>(object body)
        {
            _request.Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(body), Encoding.UTF8, "application/json");
            _request.Method = HttpMethod.Put;

            Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " " + _request.ToRestClientRequest(_restClient.BaseAddress).ToString(), FILE_LOG);
            var response = _restClient.SendAsync(_request).Result;
            Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " " + response.ToRestClientRequest().ToString(), FILE_LOG);

            var json = response.Content.ReadAsStringAsync().Result;
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json);
        }

        public IRestClient UseAuthenticationBasic(string username, string password)
        {
            var authData = string.Format("{0}:{1}", username, password);
            var authHeaderValue = Convert.ToBase64String(Encoding.UTF8.GetBytes(authData));
            _restClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", authHeaderValue);
            return this;
        }

        public IRestClient UseAuthenticationBasic(string tokenHash)
        {
            var token = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{tokenHash}:"));
            _restClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", token);
            return this;
        }

        private T DoSend<T>(HttpMethod method)
        {
            try
            {
                _request.Method = method;
                var req = _request.ToRestClientRequest(_restClient.BaseAddress);
                Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " " + req.ToString(), FILE_LOG);
                HttpResponseMessage response = _restClient.SendAsync(_request).Result;
                var resp = response.ToRestClientRequest();
                Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " " + resp.ToString(), FILE_LOG);

                if (response.IsSuccessStatusCode)
                    return Deserialize<T>(response);

                var errorMessage = response.Content.ReadAsStringAsync().Result;
                if (response.StatusCode == 0 && errorMessage.Contains("Section=ResponseStatusLine"))
                {
                    return Deserialize<T>(response);
                }

                throw RestClientFactory.CreateFailedException(_restClient, req, resp, response);
            }
            catch (HttpRequestException ex)
            {
                Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " ERROR: " + _request.ToRestClientRequest(_restClient.BaseAddress).ToString(), FILE_LOG);
                Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " EXCEP: " + ex.Message, FILE_LOG);
                throw RestClientFactory.CreateFailedException(_restClient, _request, ex);
            }
        }

        private void DoSend(HttpMethod method, object body)
        {
            _request.Method = method;

            try
            {
                _request.Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(body), Encoding.UTF8, "application/json");
                var req = _request.ToRestClientRequest(_restClient.BaseAddress);
                Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " " + req.ToString(), FILE_LOG);
                var response = _restClient.SendAsync(_request).Result;
                var resp = response.ToRestClientRequest();
                Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " " + resp.ToString(), FILE_LOG);

                var errorMessage = response.Content.ReadAsStringAsync().Result;                

                if (response.StatusCode == 0 && errorMessage.Contains("Section=ResponseStatusLine"))
                {
                    return;
                }
                
                if (!response.IsSuccessStatusCode)
                    throw RestClientFactory.CreateFailedException(_restClient, req, resp, response);
            }
            catch (HttpRequestException ex)
            {
                Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " ERROR: " + _request.ToRestClientRequest(_restClient.BaseAddress).ToString(), FILE_LOG);
                Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " EXCEP: " + ex.Message, FILE_LOG);
                throw RestClientFactory.CreateFailedException(_restClient, _request, ex);
            }
        }

        private T DoSend<T>(HttpMethod method, object body)
        {
            _request.Method = method;

            try
            {
                _request.Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(body), Encoding.UTF8, "application/json");
                var req = _request.ToRestClientRequest(_restClient.BaseAddress);
                Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " " + req.ToString(), FILE_LOG);
                var response = _restClient.SendAsync(_request).Result;
                var resp = response.ToRestClientRequest();
                Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " " + resp.ToString(), FILE_LOG);

                if (response.IsSuccessStatusCode)
                    return Deserialize<T>(response);

                var errorMessage = response.Content.ReadAsStringAsync().Result;
                if (response.StatusCode == 0 && errorMessage.Contains("Section=ResponseStatusLine"))
                {
                    return Deserialize<T>(response);
                }

                throw RestClientFactory.CreateFailedException(_restClient, req, resp, response);
            }
            catch (HttpRequestException ex)
            {
                Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " ERROR: " + _request.ToRestClientRequest(_restClient.BaseAddress).ToString(), FILE_LOG);
                Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " EXCEP: " + ex.Message, FILE_LOG);
                throw RestClientFactory.CreateFailedException(_restClient, _request, ex);
            }
        }

        private T Deserialize<T>(HttpResponseMessage response)
        {
            try
            {
                var json = response.Content.ReadAsStringAsync().Result;
                return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json);
            }
            catch (Exception ex)
            {
                Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " ERROR: " + _request.ToRestClientRequest(_restClient.BaseAddress).ToString(), FILE_LOG);
                Logging.WriteToFileThreadSafe(DateTime.Now.ToString("o") + " EXCEP: " + ex.Message, FILE_LOG);
                throw RestClientFactory.CreateErrorException(_restClient, response, ex);
            }
        }
    }
}