using Sisfarma.RestClient;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using System.Collections.Generic;
using System.Linq;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class HuecosExternalService : FisiotesExternalService, IHuecosExternalService
    {
        public HuecosExternalService(IRestClient restClient, FisiotesConfig config) 
            : base(restClient, config)
        { }

        public bool Any(int value)
        {
            var response = _restClient
                .Resource(_config.Huecos.Exists.Replace("{hueco}", $"{value}"))
                .SendGet<HuecoExists>();

            return response.Exists;
        }

        public void Insert(string[] huecos)
        {
            _restClient
                .Resource(_config.Huecos.Insert)
                .SendPut(new { ids = huecos });
        }

        public IEnumerable<string> GetByOrderAsc()
        {
            var response = _restClient
                .Resource(_config.Huecos.GetAll)
                .SendGet<IEnumerable<HuecoNumerado>>();

            return response.Select(x => x.Hueco).OrderBy(x => x);
        }

        public void Delete(string hueco)
        {
            _restClient
                .Resource(_config.Huecos.Delete)
                .SendPut(new { id = hueco });
        }        
    }

    public class HuecoNumerado
    {
        public string Hueco { get; set; }
    }

    public class HuecoExists
    {
        public string Hueco { get; set; }

        public bool Exists { get; set; }
    }
}