using System.Net.Http;

namespace Sisfarma.RestClient.WebClient.Extensions
{
    public static class RestResponseExtension
    {
        public static Response ToRestClientRequest(this HttpResponseMessage @this)
        {
            var body = @this.Content.ReadAsStringAsync().Result;
            return new Response(body, (int)@this.StatusCode);
        }
    }
}