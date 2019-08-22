using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IListasExternalService
    {
        IListasArticulosExternalService DeArticulos { get; set; }

        Lista GetCodPorDondeVoyOrDefault();
        Lista GetOneOrDefault(int codigo);
        void Sincronizar(Lista ll);
        void ResetPorDondeVoy();
    }
}