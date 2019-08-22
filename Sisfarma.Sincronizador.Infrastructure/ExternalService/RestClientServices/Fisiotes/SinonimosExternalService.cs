using Sisfarma.RestClient;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System.Collections.Generic;
using System.Linq;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class SinonimosExternalService : FisiotesExternalService, ISinonimosExternalService
    {        
        public SinonimosExternalService(IRestClient restClient, FisiotesConfig config) :
            base(restClient, config)
        { }

        public bool IsEmpty()
        {
            return _restClient
                .Resource(_config.Sinonimos.IsEmpty)
                .SendGet<IsEmptyResponse>()
                    .isEmpty;
        }

        internal class IsEmptyResponse{
            public int count { get; set; }
            public bool isEmpty { get; set; }
        }

        public void Empty()
        {
            _restClient
                .Resource(_config.Sinonimos.Empty)
                .SendPut();                    
        }

        public void Sincronizar(List<Sinonimo> items)
        {
            var sinonimos = items.Select(item => new
            {
                cod_barras = item.cod_barras.Strip(),
                cod_nacional = item.cod_nacional.Strip()
            });

            _restClient
                .Resource(_config.Sinonimos.Insert)
                .SendPost(new { bulk = sinonimos });
        }
    }
}