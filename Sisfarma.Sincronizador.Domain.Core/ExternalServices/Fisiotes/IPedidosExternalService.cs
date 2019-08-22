using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IPedidosExternalService
    {
        bool Exists(int pedido);
        bool ExistsLinea(int pedido, int linea);
        Pedido Get(int pedido);
        LineaPedido GetLineaByKey(int pedido, int linea);
        void Sincronizar(Pedido pp);
        void Sincronizar(LineaPedido ll);
        Pedido LastOrDefault();
    }
}