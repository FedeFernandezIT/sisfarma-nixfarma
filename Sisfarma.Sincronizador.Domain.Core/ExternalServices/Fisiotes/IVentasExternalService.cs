using System.Collections.Generic;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes.DTO.VentasPendientes;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IVentasExternalService
    {
        void Sincronizar(IEnumerable<VentaPendiente> ventasPendientes);

        IEnumerable<VentaPendiente> GetAllPendientes();

        void Sincronizar(IEnumerable<DeleteVentaPendiente> deleteVentasPendientes);
    }
}