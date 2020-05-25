using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IPedidosExternalService
    {
        bool Exists(int pedido);
        bool ExistsLinea(int pedido, int linea);
        Pedido Get(int pedido);
        LineaPedido GetLineaByKey(int pedido, int linea);
        void Sincronizar(IEnumerable<Pedido> pps);
        void Sincronizar(IEnumerable<LineaPedido> lls);
        Pedido LastOrDefault();
    }
}