using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.RestClient
{
    public class Request
    {
        public string Url { get; private set; }

        public string MethodHttp { get; private set; }

        public string Body { get; private set; }

        public Request(string url, string methodHttp, string body)
        {
            Url = url ?? throw new ArgumentNullException(nameof(url));
            MethodHttp = methodHttp ?? throw new ArgumentNullException(nameof(methodHttp));
            Body = body;
        }

        public override string ToString() 
            => $@"[{MethodHttp}] {Url}{Environment.NewLine}{Body}";
    }
}
