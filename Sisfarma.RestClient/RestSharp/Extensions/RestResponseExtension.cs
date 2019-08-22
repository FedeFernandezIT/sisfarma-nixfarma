using RestSharp;

namespace Sisfarma.RestClient.RestSharp.Extensions
{
    public static class RestResponseExtension
    {
        public static Response ToRestClientRequest(this IRestResponse @this) 
            => new Response(@this.Content, (int)@this.StatusCode);        
    }
}
