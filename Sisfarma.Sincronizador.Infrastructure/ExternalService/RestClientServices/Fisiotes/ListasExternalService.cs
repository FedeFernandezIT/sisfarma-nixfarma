using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class ListasExternalService : FisiotesExternalService, IListasExternalService
    {
        public IListasArticulosExternalService DeArticulos { get; set; }

        public ListasExternalService(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        {
            DeArticulos = new ListasArticulosExternalService(restClient, config);
        }

        public ListasExternalService(IRestClient restClient, FisiotesConfig config, IListasArticulosExternalService listaDeArticulos)
            : base(restClient, config)
        {
            DeArticulos = listaDeArticulos ?? throw new System.ArgumentNullException(nameof(listaDeArticulos));
        }

        public Lista GetCodPorDondeVoyOrDefault()
        {
            try
            {
                return _restClient
                    .Resource(_config.Listas.PorDondeVoyActual)
                    .SendGet<Lista>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public void Sincronizar(Lista ll)
        {
            var lista = new
            {
                cod = ll.cod,
                lista = ll.lista.Strip(),
                numArticulos = ll.numArticulos,
                porDondeVoy = 1
            };

            _restClient
                .Resource(_config.Listas.InsertOrUpdate)
                .SendPost(new
                {
                    bulk = new[] { lista }
                });
        }

        public Lista GetOneOrDefault(int codigo)
        {
            try
            {
                return _restClient
                    .Resource(_config.Listas.GetByCodigo
                        .Replace("{codigo}", $"{codigo}"))
                    .SendGet<Lista>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public void ResetPorDondeVoy()
        {
            try
            {
                _restClient
                    .Resource(_config.Listas.ResetPorDondeVoy)
                    .SendPost();
            }
            catch (RestClientNotFoundException)
            {
                return;
            }
        }
    }
}