using System.Threading.Tasks;

namespace Sisfarma.RestClient
{
    public interface IRestClient
    {
        IRestClient BaseAddress(string ulr);

        IRestClient Resource(string resource);

        IRestClient UseAuthenticationBasic(string username, string password);

        IRestClient UseAuthenticationBasic(string token);

        Task<T> SendGetAsync<T>();

        T SendGet<T>();

        void SendGet();

        Task<T> SendPostAsync<T>(object body);

        T SendPost<T>(object body);

        void SendPost(object body = null);

        void SendPut(object body = null);

        T SendPut<T>(object body);

        void SendDelete(object body = null);
    }
}