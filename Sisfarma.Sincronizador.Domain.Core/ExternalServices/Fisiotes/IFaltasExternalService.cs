using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IFaltasExternalService : IFaltasExternalServiceNew
    {
        bool ExistsLineaDePedido(long idPedido, int idLinea);

        Falta GetByLineaDePedido(long pedido, int linea);

        void Insert(Falta ff);

        Falta LastOrDefault();
    }

    public interface IFaltasExternalServiceNew
    {
        void Sincronizar(Falta falta);
    }
}