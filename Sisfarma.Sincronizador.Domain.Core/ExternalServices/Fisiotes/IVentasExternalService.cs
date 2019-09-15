using System.Collections;
using System.Collections.Generic;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes.DTO.VentasPendientes;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IVentasExternalService
    {
        void Sincronizar(VentaPendiente ventaPendiente);

        IEnumerable<VentaPendiente> GetAllPendientes();

        void Sincronizar(DeleteVentaPendiente deleteVentaPendiente);
    }
}