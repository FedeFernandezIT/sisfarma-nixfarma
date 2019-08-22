using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IFaltasExternalService : IFaltasExternalServiceNew
    {
        bool ExistsLineaDePedido(int idPedido, int idLinea);
        Falta GetByLineaDePedido(int pedido, int linea);
        void Insert(Falta ff);
        Falta LastOrDefault();
    }

    public interface IFaltasExternalServiceNew
    {
        void Sincronizar(Falta falta);
    }
}