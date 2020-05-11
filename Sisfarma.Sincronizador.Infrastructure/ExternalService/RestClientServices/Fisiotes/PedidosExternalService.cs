using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class PedidosExternalService : FisiotesExternalService, IPedidosExternalService
    {        
        public PedidosExternalService(IRestClient restClient, FisiotesConfig config) 
            : base(restClient, config)
        { }

        public Pedido LastOrDefault()
        {
            try
            {
                return _restClient
                .Resource(_config.Pedidos.Ultimo)
                .SendGet<Pedido>();                    
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }            
        }


        public bool Exists(int pedido)
        {
            return Get(pedido) != null;
        }

        public Pedido Get(int pedido)
        {
            try
            {
                return _restClient
                .Resource(_config.Pedidos.GetByPedido.Replace("{pedido}", $"{pedido}"))
                .SendGet<Pedido>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public void Sincronizar(Pedido pp)
        {
            var pedido = new
            {
                idPedido = pp.idPedido,
                fechaPedido = pp.fechaPedido.ToIsoString(),
                hora = pp.hora.ToIsoString(),
                numLineas = pp.numLineas,
                importePvp = pp.importePvp,
                importePuc = pp.importePuc,
                idProveedor = pp.idProveedor,
                proveedor = pp.proveedor,
                trabajador = pp.trabajador
            };

            _restClient
                .Resource(_config.Pedidos.Insert)
                .SendPost(new
                {
                    bulk = new[] { pedido }
                });            
        }


        public bool ExistsLinea(int pedido, int linea)
        {
            return GetLineaByKey(pedido, linea) != null;
        }

        public LineaPedido GetLineaByKey(int pedido, int linea)
        {
            try
            {
                return _restClient
                .Resource(_config.Pedidos.GetByLineaDePedido
                    .Replace("{pedido}", $"{pedido}")
                    .Replace("{linea}", $"{linea}"))
                .SendGet<LineaPedido>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }            
        }


        public void Sincronizar(LineaPedido ll)
        {
            var linea = new
            {
                fechaPedido = ll.fechaPedido.ToIsoString(),
                idPedido = ll.idPedido,
                idLinea = ll.idLinea,
                cod_nacional = ll.cod_nacional,
                descripcion = ll.descripcion.Strip(),
                familia = ll.familia,
                superFamilia = ll.superFamilia,                
                pvp = ll.pvp,
                puc = ll.puc,
                cod_laboratorio = ll.cod_laboratorio,
                laboratorio = ll.laboratorio
            };

            _restClient
                .Resource(_config.Pedidos.InsertLineaDePedido)
                .SendPost(new
                {
                    bulk = new[] { linea }
                });
        }
    }
}