using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class FaltasExternalService : FisiotesExternalService, IFaltasExternalService
    {
        public FaltasExternalService(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        { }

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
            var falta = new
            {
                idPedido = ff.idPedido,
                idLinea = ff.idLinea,
                cod_nacional = ff.cod_nacional,
                descripcion = ff.descripcion,
                familia = ff.familia,
                superFamilia = ff.superFamilia,
                cantidadPedida = ff.cantidadPedida,
                fechaFalta = ff.fechaFalta.ToIsoString(),
                cod_laboratorio = ff.cod_laboratorio,
                laboratorio = ff.laboratorio,
                proveedor = ff.proveedor,
                fechaPedido = ff.fechaPedido.ToIsoString(),
                pvp = ff.pvp,
                puc = ff.puc
            };

            _restClient
                .Resource(_config.Faltas.InsertLineaDePedido)
                .SendPost(new
                {
                    bulk = new[] { falta }
                });
        }

        public void Sincronizar(Falta falta)
        {
            throw new System.NotImplementedException();
        }
    }
}