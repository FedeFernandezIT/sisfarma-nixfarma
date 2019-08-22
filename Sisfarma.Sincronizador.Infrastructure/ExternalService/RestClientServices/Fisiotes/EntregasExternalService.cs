using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class EntregasExternalService : FisiotesExternalService, IEntregasExternalService
    {        
        public EntregasExternalService(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        { }

        public bool Exists(int venta, int linea)
        {
            try
            {
                _restClient.Resource(_config.Entregas.GetByKey
                    .Replace("{venta}", $"{venta}")
                    .Replace("{linea}", $"{linea}"))
                        .SendGet();
                return true;
            }
            catch (RestClientNotFoundException)
            {
                return false;
            }            
        }


        public void Insert(EntregaCliente ec)
        {
            _restClient
                .Resource(_config.Entregas.Insert)
                .SendPost(new
                {
                    idventa = ec.idventa,
                    idnlinea = ec.idnlinea,
                    codigo = ec.codigo,
                    descripcion = ec.descripcion,
                    cantidad = ec.cantidad,
                    precio = ec.precio,
                    tipo = ec.tipo,
                    fecha = ec.fecha,
                    dni = ec.dni,
                    puesto = ec.puesto,
                    trabajador = ec.trabajador,
                    fechaEntrega = ec.fechaEntrega.ToIsoString(),
                    pvp = ec.pvp
                });
        }

        public void Insert(int venta, int linea, string codigo, string descripcion, int cantidad, decimal numero, string tipoLinea, int fecha,
                string dni, string puesto, string trabajador, DateTime fechaVenta, float? pvp)
        {            
            _restClient
                .Resource(_config.Entregas.Insert)
                .SendPost(new
                {
                    idventa = venta,
                    idnlinea = linea,
                    codigo = codigo,
                    descripcion = descripcion,
                    cantidad = cantidad,
                    precio = numero,
                    tipo = tipoLinea,
                    fecha = fecha,
                    dni = dni,
                    puesto = puesto,
                    trabajador = trabajador,
                    fechaEntrega = fechaVenta.ToIsoString(),
                    pvp = pvp                    
                });
        }        
    }
}