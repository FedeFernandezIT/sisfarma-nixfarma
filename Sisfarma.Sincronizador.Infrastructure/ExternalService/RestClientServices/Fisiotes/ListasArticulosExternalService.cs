using Sisfarma.RestClient;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System.Collections.Generic;
using System.Linq;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class ListasArticulosExternalService : FisiotesExternalService, IListasArticulosExternalService
    {
        public ListasArticulosExternalService(IRestClient restClient, FisiotesConfig config) :
            base(restClient, config)
        { }

        public void Delete(long codigo)
        {
            _restClient
                .Resource(_config.ListaDeArticulos.Eliminar)
                .SendPut(new { ids = new[] { codigo } });
        }

        public void Sincronizar(List<ListaArticulo> items)
        {
            var articulos = items.Select(i => new
            {
                cod_lista = i.cod_lista,
                cod_articulo = i.cod_articulo
            });

            _restClient
                .Resource(_config.ListaDeArticulos.Insert)
                .SendPost(new
                {
                    bulk = articulos
                });
        }

        public void Sincronizar(ListaArticulo la)
        {
            Sincronizar(new List<ListaArticulo> { la });
        }
    }
}