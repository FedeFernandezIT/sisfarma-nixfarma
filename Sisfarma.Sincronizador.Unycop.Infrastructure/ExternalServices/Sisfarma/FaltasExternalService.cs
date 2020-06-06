using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes;
using System.Collections.Generic;
using System.Linq;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.ExternalServices.Sisfarma
{
    public class FaltasExternalService : FisiotesExternalService, IFaltasExternalService
    {
        public FaltasExternalService(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        { }

        public Falta GetByLineaDePedido(long pedido, int linea)
        {
            try
            {
                return _restClient
                .Resource(_config.Faltas.GetByLineaDePedido
                    .Replace("{pedido}", $"{pedido}")
                    .Replace("{linea}", $"{linea}"))
                .SendGet<Falta>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public bool ExistsLineaDePedido(long idPedido, int idLinea)
        {
            return GetByLineaDePedido(idPedido, idLinea) != null;
        }

        public void Insert(Falta ff)
        {
            throw new System.NotImplementedException();
        }

        public Falta LastOrDefault()
        {
            try
            {
                return _restClient
                .Resource(_config.Faltas.Ultima)
                .SendGet<Falta>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public void Sincronizar(IEnumerable<Falta> ffs)
        {
            var bulk = ffs.Select(ff => new
            {
                idPedido = ff.idPedido,
                idLinea = ff.idLinea,
                cod_nacional = ff.cod_nacional,
                descripcion = ff.descripcion.Strip(),
                familia = ff.familia.Strip(),
                superFamilia = ff.superFamilia.Strip(),
                cantidadPedida = ff.cantidadPedida,
                fechaFalta = ff.fechaFalta.ToIsoString(),
                cod_laboratorio = ff.cod_laboratorio.Strip(),
                laboratorio = ff.laboratorio.Strip(),
                proveedor = ff.proveedor.Strip(),
                fechaPedido = ff.fechaPedido.ToIsoString(),
                pvp = ff.pvp,
                puc = ff.puc,
                sistema = ff.sistema,
                categoria = ff.categoria.Strip(),
                subcategoria = ff.subcategoria.Strip()
            });

            _restClient
                .Resource(_config.Faltas.InsertLineaDePedido)
                .SendPost(new
                {
                    bulk = bulk
                });
        }
    }
}