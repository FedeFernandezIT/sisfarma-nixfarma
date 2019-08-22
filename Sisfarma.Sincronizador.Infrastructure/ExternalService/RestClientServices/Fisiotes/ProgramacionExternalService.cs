using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class ProgramacionExternalService : FisiotesExternalService, IProgramacionExternalService
    {
        public ProgramacionExternalService(IRestClient restClient, FisiotesConfig config) 
            : base(restClient, config)
        { }

        public Programacion GetProgramacionOrDefault(string estado)
        {
            try
            {
                if (Programacion.Encendido == estado)                
                    return _restClient
                        .Resource(_config.Programacion.Encendido)
                        .SendGet<Programacion>();

                if (Programacion.Apagado == estado)
                    return _restClient
                        .Resource(_config.Programacion.Apagado)
                        .SendGet<Programacion>();

                return default(Programacion);
            }
            catch (RestClientNotFoundException)
            {
                return default(Programacion);
            }
        }
    }
}
