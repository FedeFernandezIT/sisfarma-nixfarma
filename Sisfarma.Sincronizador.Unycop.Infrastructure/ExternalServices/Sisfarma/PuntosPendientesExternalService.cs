using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes.DTO;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.ExternalServices.Sisfarma
{
    public class PuntosPendientesExternalService : FisiotesExternalService, IPuntosPendientesExternalService
    {
        public PuntosPendientesExternalService(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        { }

        public bool Exists(int venta, int linea)
        {
            throw new NotImplementedException();
        }

        public bool ExistsGreatThanOrEqual(DateTime fecha)
        {
            var year = fecha.Year;
            var fechaVenta = fecha.Date.ToIsoString();

            try
            {
                return _restClient
                    .Resource(_config.Puntos.ExistsByFechaGreatThanOrEqual
                        .Replace("{year}", $"{year}")
                        .Replace("{fecha}", $"{fechaVenta})"))
                    .SendGet<bool>();
            }
            catch (RestClientNotFoundException)
            {
                return false;
            }
        }

        public long GetLastOfYear(int year)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<PuntosPendientes> GetOfRecetasPendientes(int año)
        {
            throw new NotImplementedException();
        }

        public PuntosPendientes GetOneOrDefaultByItemVenta(int venta, int linea)
        {
            throw new NotImplementedException();
        }

        public decimal GetPuntosByDni(int dni)
        {
            throw new NotImplementedException();
        }

        public decimal GetPuntosCanjeadosByDni(int dni)
        {
            throw new NotImplementedException();
        }

        public DateTime GetTimestampUltimaVentaByEmpresa(string empresa)
        {
            try
            {
                return _restClient
                    .Resource(_config.Puntos.GetTimestampUltimaVenta.Replace("{empresa}", $"{empresa}"))
                    .SendGet<FechaUltimaVenta>()
                        .fechaVenta.ToDateTimeOrDefault("yyyy-MM-dd HH:mm:ss");
            }
            catch (RestClientNotFoundException)
            {
                return DateTime.MinValue;
            }
        }

        internal class FechaUltimaVenta
        {
            public string fechaVenta { get; set; }
        }

        public IEnumerable<PuntosPendientes> GetWithoutRedencion()
        {
            try
            {
                return _restClient
                    .Resource(_config.Puntos.GetSinRedencion)
                    .SendGet<IEnumerable<DTO.PuntosPendientes>>()
                        .ToList()
                        .Select(x => new PuntosPendientes { VentaId = x.idventa });
            }
            catch (RestClientNotFoundException)
            {
                return new List<PuntosPendientes>();
            }
        }

        public IEnumerable<PuntosPendientes> GetWithoutTicket()
        {
            try
            {
                return _restClient
                    .Resource(_config.Puntos.GetSinRedencion)
                    .SendGet<IEnumerable<DTO.PuntosPendientes>>()
                        .ToList()
                        .Select(x => new PuntosPendientes { VentaId = x.idventa });
            }
            catch (RestClientNotFoundException)
            {
                return new List<PuntosPendientes>();
            }
        }

        public void Insert(IEnumerable<PuntosPendientes> pps)
        {
            throw new NotImplementedException();
        }

        public void Insert(int venta, int linea, string codigoBarra, string codigo, string descripcion, string familia, int cantidad, decimal numero, string tipoPago, int fecha, string dni, string cargado, string puesto, string trabajador, string codLaboratorio, string laboratorio, string proveedor, string receta, DateTime fechaVenta, string superFamlia, float precioMed, float pcoste, float dtoLinea, float dtoVta, float redencion, string recetaPendiente)
        {
            throw new NotImplementedException();
        }

        public void Insert(PuntosPendientes pp)
        {
            throw new NotImplementedException();
        }

        public void InsertPuntuacion(InsertPuntuacion pp)
        {
            throw new NotImplementedException();
        }

        public void Sincronizar(PuntosPendientes pp, bool calcularPuntos = false)
        {
            var set = new
            {
                idventa = pp.VentaId,
                idnlinea = pp.LineaNumero,
                cod_barras = pp.CodigoBarra,
                cod_nacional = pp.CodigoNacional,
                descripcion = pp.Descripcion.Strip(),
                familia = pp.Familia,
                cantidad = pp.Cantidad,
                precio = pp.Precio,
                tipoPago = pp.TipoPago,
                fecha = pp.Fecha,
                dni = pp.DNI,
                cargado = pp.Cargado,
                puesto = pp.Puesto,
                trabajador = pp.Trabajador,
                cod_laboratorio = pp.LaboratorioCodigo,
                laboratorio = pp.Laboratorio,
                proveedor = pp.Proveedor,
                receta = pp.Receta,
                fechaVenta = pp.FechaVenta.ToIsoString(),
                superFamilia = pp.SuperFamilia,
                pvp = pp.PVP,
                puc = pp.PUC,
                pago = pp.Pago,
                categoria = pp.Categoria,
                subcategoria = pp.Subcategoria,
                sistema = pp.Sistema,
                dtoLinea = pp.LineaDescuento,
                dtoVenta = pp.VentaDescuento,
                actualizado = "1",
                numTicket = pp.TicketNumero ?? -1,
                serie = pp.Serie,
                superFamiliaAux = pp.SuperFamiliaAux,
                familiaAux = pp.FamiliaAux,
                cambioClasificacion = pp.CambioClasificacion,
                ubicacion = pp.Ubicacion
            };

            var where = new { idventa = pp.VentaId, idnlinea = pp.LineaNumero };

            _restClient
                .Resource(calcularPuntos ? _config.Puntos.InsertActualizarVenta : _config.Puntos.Insert)
                .SendPost(new
                {
                    puntos = new[] { new { set, where } }
                });
        }

        public void Sincronizar(IEnumerable<PuntosPendientes> pp)
        {
        }

        public void Update(long venta)
        {
            throw new NotImplementedException();
        }

        public void Update(long venta, long linea, string receta = "C")
        {
            throw new NotImplementedException();
        }

        public void Update(string tipoPago, string proveedor, float? dtoLinea, float? dtoVenta, float redencion, long venta, long linea)
        {
            throw new NotImplementedException();
        }

        public void Sincronizar(UpdatePuntacion pp)
        {
            if (pp.cod_nacional == null)
            {
                var set = new
                {
                    pp.tipoPago,
                    actualizado = 1
                };

                var where = new { idventa = pp.idventa };

                _restClient
                   .Resource(_config.Puntos.Update)
                   .SendPut(new
                   {
                       puntos = new { set, where }
                   });
            }
            else
            {
                var set = new
                {
                    pp.tipoPago,
                    pp.proveedor,
                    actualizado = 1
                };

                var where = new { idventa = pp.idventa, cod_nacional = pp.cod_nacional };

                _restClient
                   .Resource(_config.Puntos.Update)
                   .SendPut(new
                   {
                       puntos = new { set, where }
                   });
            }
        }

        public void Sincronizar(UpdateTicket tk)
        {
            var set = new
            {
                tk.numTicket,
                tk.serie
            };

            var where = new { idventa = tk.idventa };

            _restClient
               .Resource(_config.Puntos.Update)
               .SendPut(new
               {
                   puntos = new { set, where }
               });
        }

        public bool AnyWithoutPagoGreaterThanVentaId(long ultimaVenta)
        {
            try
            {
                return _restClient
                    .Resource(_config.Puntos.GetSinRedencion)
                    .SendGet<IEnumerable<DTO.PuntosPendientes>>()
                        .ToList()
                        .Any();
            }
            catch (RestClientNotFoundException)
            {
                return false;
            }
        }
    }
}