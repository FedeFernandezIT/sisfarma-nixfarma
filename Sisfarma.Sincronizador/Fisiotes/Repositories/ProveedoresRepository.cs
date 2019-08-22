using System;
using System.Collections.Generic;
using System.Linq;
using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Fisiotes.Models;

namespace Sisfarma.Sincronizador.Fisiotes.Repositories
{
    public class ProveedoresRepository : FisiotesRepository
    {
        public ProveedoresRepository(IRestClient restClient, FisiotesConfig config) 
            : base(restClient, config)
        {
        }

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
        
        public void Insert(Proveedor pp)
        {
            var proveedor = new
            {
                idProveedor = pp.idProveedor,
                nombre = pp.nombre
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

        public void InsertHistorico(IEnumerable<ProveedorHistorial> items)
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
