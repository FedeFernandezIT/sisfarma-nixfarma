using System;
using System.Collections.Generic;
using System.Linq;
using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class ProveedoresExternalService : FisiotesExternalService, IProveedoresExternalService
    {
        public ProveedoresExternalService(IRestClient restClient, FisiotesConfig config) 
            : base(restClient, config)
        { }

        public DateTime? GetFechaMaximaDeHistorico()
        {
            return _restClient
                .Resource(_config.Proveedores.GetFechaMaximaHistorial)
                .SendGet<FechaMaxima>()
                    .fecha;
        }

        internal class FechaMaxima
        {
            public DateTime? fecha { get; set; }
        }
        
        public void Sincronizar(Proveedor pp)
        {
            var proveedor = new
            {
                idProveedor = pp.idProveedor,
                nombre = pp.nombre.Strip()
            };

            _restClient
                .Resource(_config.Proveedores.Insert)
                .SendPost(new { bulk = new[] { proveedor } });
        }

        public void Update(Proveedor pp)
        {
            var proveedor = new
            {
                id = pp.id,
                idProveedor = pp.idProveedor,
                nombre = pp.nombre
            };

            _restClient
                .Resource(_config.Proveedores.Update)
                .SendPost(new { bulk = new[] { proveedor } });
        }

        public Proveedor GetOneOrDefault(string proveedor, string nombre)
        {
            try
            {
                return _restClient
                    .Resource(_config.Proveedores.GetOneByProveedor
                        .Replace("{proveedor}", proveedor)
                        .Replace("{nombre}", nombre))
                    .SendGet<Proveedor>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public void Sincronizador(IEnumerable<ProveedorHistorial> items)
        {
            var historicos = items.Select(item => new
            {
                idProveedor = item.idProveedor,
                cod_nacional = item.cod_nacional,
                fecha = item.fecha.ToIsoString(),
                puc = item.puc
            });
                            
            _restClient
                .Resource(_config.Proveedores.InsertHistorico)
                .SendPost(new { bulk = historicos });
        }
    }
}
