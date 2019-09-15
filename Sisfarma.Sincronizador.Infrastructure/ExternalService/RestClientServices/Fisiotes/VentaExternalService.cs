using System.Collections.Generic;
using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes.DTO.VentasPendientes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes;

namespace Sisfarma.Sincronizador.Infrastructure.ExternalService.Fisiotes
{
    public class VentasExternalService : FisiotesExternalService, IVentasExternalService
    {
        public VentasExternalService(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        { }

        public IEnumerable<VentaPendiente> GetAllPendientes()
        {
            try
            {
                return _restClient
                    .Resource(_config.Ventas.GetAllPendientes)
                    .SendGet<IEnumerable<VentaPendiente>>();
            }
            catch (RestClientNotFoundException)
            {
                return new List<VentaPendiente>();
            }
        }

        public void Sincronizar(VentaPendiente ventaPendiente)
        {
            _restClient
                .Resource(_config.Ventas.InsertVentaPendiente)
                .SendPost(new
                {
                    venta = new { idventa = ventaPendiente.idventa, empresa = ventaPendiente.empresa }
                });
        }

        public void Sincronizar(DeleteVentaPendiente ventaPendiente)
        {
            _restClient
                .Resource(_config.Ventas.DeleteVentaPendiente)
                .SendDelete(new
                {
                    venta = new { idventa = ventaPendiente.idventa, empresa = ventaPendiente.empresa }
                });
        }
    }
}