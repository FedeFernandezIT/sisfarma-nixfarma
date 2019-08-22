using RSharp = RestSharp;
using RestSharp.Authenticators;
using System;
using System.Linq;
using System.Text;

namespace Sisfarma.RestClient.RestSharp
{
    public class HttpBasicAuthentication : IAuthenticator
    {
        private readonly string authHeader;

        public HttpBasicAuthentication(string tokenHash)
        {
            var token = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{tokenHash}:"));

            authHeader = string.Format("Basic {0}", token);
        }

        public void Authenticate(RSharp.IRestClient client, RSharp.IRestRequest request)
        {            
            if (!request.Parameters.Any(p => "Authorization".Equals(p.Name, StringComparison.OrdinalIgnoreCase)))
                request.AddParameter("Authorization", authHeader, RSharp.ParameterType.HttpHeader);
        }
    }
}
