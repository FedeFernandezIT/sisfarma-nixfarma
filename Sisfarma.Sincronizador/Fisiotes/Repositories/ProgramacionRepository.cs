using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Fisiotes.Models;

namespace Sisfarma.Sincronizador.Fisiotes.Repositories
{
    public class ProgramacionRepository : FisiotesRepository
    {
        public ProgramacionRepository(IRestClient restClient, FisiotesConfig config) 
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
