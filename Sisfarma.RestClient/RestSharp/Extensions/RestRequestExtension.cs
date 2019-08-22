using Newtonsoft.Json;
using RestSharp;
using System;
using System.Linq;

namespace Sisfarma.RestClient.RestSharp.Extensions
{
    public static class RestRequestExtension
    {
        public static Request ToRestClientRequest(this IRestRequest @this, Uri baseAddress)
        {
            var body = @this.Parameters
                .FirstOrDefault(p => p.Type == ParameterType.RequestBody);


            var bodyJson = string.Empty;
            if (body != null)
            {
                bodyJson = JsonConvert.SerializeObject(body.Value);
            }

            

            var url = $"{baseAddress}/{@this.Resource}";

            return new Request(url, @this.Method.ToString(), bodyJson);
        }
    }
}
