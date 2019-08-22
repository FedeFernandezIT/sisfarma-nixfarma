using Sisfarma.RestClient;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class ConfiguracionesExternalService : FisiotesExternalService, IConfiguracionesExternalService
    {
        public ConfiguracionesExternalService(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        { }

        public string GetByCampo(string field)
        {
            return _restClient
                .Resource(_config.Configuraciones.GetValorByCampo.Replace("{campo}", field))
                .SendGet<string>();
        }

        public void Update(string field, string value)
        {
            _restClient
                .Resource(_config.Configuraciones.UpdateValorByCampo)
                .SendPut(new { campo = field, valor = value });
        }

        public bool PerteneceFarmazul()
        {
            return _restClient
               .Resource(_config.Configuraciones.PerteneceFarmazul)
               .SendGet<bool>();
        }

        public IEnumerable<Configuracion> GetEstadosActuales()
        {
            return _restClient
               .Resource(_config.Configuraciones.GetAll)
               .SendGet<IEnumerable<Configuracion>>();
        }                        
    }
}