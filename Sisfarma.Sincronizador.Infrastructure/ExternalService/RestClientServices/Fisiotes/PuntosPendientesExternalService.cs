using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes.DTO;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class PuntosPendientesExternalService : FisiotesExternalService
    {        
        public PuntosPendientesExternalService(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        { }

        public IEnumerable<PuntosPendientes> GetOfRecetasPendientes(int año)
        {
            return _restClient
                .Resource(_config.Puntos.GetVentasRecetasPendientes
                    .Replace("{anio}", $"{año}"))
               .SendGet<IEnumerable<PuntosPendientes>>();
        }

        public void Update(long venta)
        {
            var set = new { tipoPago = "C", redencion = 0 };
            var where = new { idventa = venta };

            _restClient
               .Resource(_config.Puntos.Update)
               .SendPut(new
               {
                   puntos = new { set, where }
               });
        }

        public void Update(long venta, long linea, string receta = "C")
        {
            var set = new { recetaPendiente = receta, actualizado = 1 };
            var where = new { idventa = venta, idnlinea = linea };

            _restClient
               .Resource(_config.Puntos.Update)
               .SendPut(new
               {
                   puntos = new { set, where }
               });
        }

        public void Update(string tipoPago, string proveedor, float? dtoLinea, float? dtoVenta, float redencion, long venta, long linea)
        {
            var set = new
            {
                tipoPago = tipoPago,
                proveedor = proveedor,
                dtoLinea = dtoLinea,
                dtoVenta = dtoVenta,
                redencion = redencion
            };

            var where = new { idventa = venta, idnlinea = linea };

            _restClient
               .Resource(_config.Puntos.Update)
               .SendPut(new
               {
                   puntos = new { set, where }
               });
        }

        public IEnumerable<PuntosPendientes> GetWithoutRedencion()
        {
            try
            {
                return _restClient
                    .Resource(_config.Puntos.GetSinRedencion)
                    .SendGet<IEnumerable<PuntosPendientes>>();
            }
            catch (RestClientNotFoundException)
            {
                return new List<PuntosPendientes>();
            }
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
            try
            {
                return _restClient
                    .Resource(_config.Puntos.GetLastOfYear
                        .Replace("{year}", $"{year}"))
                    .SendGet<IdVentaResponse>()
                        .idventa ?? 1L;
            }
            catch (RestClientNotFoundException)
            {
                return 1L;
            }
        }

        public long GetUltimaVenta()
        {
            try
            {
                return _restClient
                    .Resource(_config.Puntos.GetUltimaVenta)
                    .SendGet<IdVentaResponse>()
                        .idventa ?? 1L;
            }
            catch (RestClientNotFoundException)
            {
                return 1L;
            }
        }

        public bool Exists(int venta, int linea)
            => GetOneOrDefaultByItemVenta(venta, linea) != null;

        public PuntosPendientes GetOneOrDefaultByItemVenta(int venta, int linea)
        {
            try
            {
                return _restClient
                    .Resource(_config.Puntos.GetByItemVenta
                        .Replace("{venta}", $"{venta}")
                        .Replace("{linea}", $"{linea}"))
                    .SendGet<PuntosPendientes>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        internal class IdVentaResponse
        {
            public long? idventa { get; set; }
        }

        public void Insert(PuntosPendientes pp)
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
                dtoLinea = pp.LineaDescuento,
                dtoVenta = pp.VentaDescuento,
                redencion = pp.redencion,
                recetaPendiente = pp.recetaPendiente
            };

            var where = new { idventa = pp.VentaId, idnlinea = pp.LineaNumero };

            _restClient
                .Resource(_config.Puntos.Insert)
                .SendPost(new
                {
                    puntos = new[] { new { set, where } }
                });
        }

        public void Insert(IEnumerable<PuntosPendientes> pps)
        {
            var puntos = pps.Select(pp =>
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
                    dtoLinea = pp.LineaDescuento,
                    dtoVenta = pp.VentaDescuento,
                    redencion = pp.redencion,
                    recetaPendiente = pp.recetaPendiente
                };

                var where = new { idventa = pp.VentaId, idnlinea = pp.LineaNumero };

                return new { set, where };
            });

            _restClient
                .Resource(_config.Puntos.Insert)
                .SendPost(new
                {
                    puntos = puntos
                });
        }

        public void Insert(int venta, int linea, string codigoBarra, string codigo, string descripcion, string familia, int cantidad, decimal numero,
                string tipoPago, int fecha, string dni, string cargado, string puesto, string trabajador, string codLaboratorio, string laboratorio, string proveedor,
                string receta, DateTime fechaVenta, string superFamlia, float precioMed, float pcoste, float dtoLinea, float dtoVta, float redencion, string recetaPendiente)
        {
            var set = new
            {
                idventa = venta,
                idnlinea = linea,
                cod_barras = codigoBarra,
                cod_nacional = codigo,
                descripcion = descripcion,
                familia = familia,
                cantidad = cantidad,
                precio = numero,
                tipoPago = tipoPago,
                fecha = fecha,
                dni = dni,
                cargado = cargado,
                puesto = puesto,
                trabajador = trabajador,
                cod_laboratorio = codLaboratorio,
                laboratorio = laboratorio,
                proveedor = proveedor,
                receta = receta,
                fechaVenta = fechaVenta.ToIsoString(),
                superFamilia = superFamlia,
                pvp = precioMed,
                puc = pcoste,
                dtoLinea = dtoLinea,
                dtoVenta = dtoVta,
                redencion = redencion,
                recetaPendiente = recetaPendiente
            };

            var where = new { idventa = venta, idnlinea = linea };

            _restClient
                .Resource(_config.Puntos.Insert)
                .SendPost(new
                {
                    puntos = new { set, where }
                });
        }

        public void UpdatePuntacion(UpdatePuntacion pp)
        {
            var set = new
            {
                pp.cantidad,
                pp.dtoLinea,
                pp.dtoVenta,
                pp.dni,
                pp.precio,
                pp.receta,
                pp.tipoPago,
                pp.trabajador
            };

            var where = new { idventa = pp.idventa, idnlinea = pp.idnlinea };

            _restClient
               .Resource(_config.Puntos.Update)
               .SendPut(new
               {
                   puntos = new { set, where }
               });
        }

        public void InsertPuntuacion(InsertPuntuacion pp)
        {
            var set = new
            {
                idventa = pp.idventa,
                idnlinea = pp.idnlinea,
                cod_barras = pp.cod_barras,
                cod_nacional = pp.cod_nacional,
                descripcion = pp.descripcion,
                familia = pp.familia,
                cantidad = pp.cantidad,
                precio = pp.precio,
                tipoPago = pp.tipoPago,
                fecha = pp.fecha,
                dni = pp.dni,
                cargado = pp.cargado,
                puesto = pp.puesto,
                trabajador = pp.trabajador,
                cod_laboratorio = pp.cod_laboratorio,
                laboratorio = pp.laboratorio,
                proveedor = pp.proveedor,
                receta = pp.receta,
                fechaVenta = pp.fechaVenta.ToIsoString(),
                superFamilia = pp.superFamilia,
                pvp = pp.pvp,
                puc = pp.puc,
                dtoLinea = pp.dtoLinea,
                dtoVenta = pp.dtoVenta,
                redencion = pp.redencion,
                recetaPendiente = pp.recetaPendiente
            };

            var where = new { idventa = pp.idventa, idnlinea = pp.idnlinea };

            _restClient
                .Resource(_config.Puntos.InsertActualizarVenta)
                .SendPost(new
                {
                    puntos = new[] { new { set, where } }
                });
        }

        public decimal GetPuntosByDni(int dni)
        {
            return _restClient
                .Resource(_config.Puntos.GetPuntosByDni.Replace("{dni}", $"{dni}"))
                .SendGet<decimal>();
        }

        public decimal GetPuntosCanjeadosByDni(int dni)
        {
            return _restClient
                .Resource(_config.Puntos.GetPuntosCanjeadosByDni.Replace("{dni}", $"{dni}"))
                .SendGet<decimal>();
        }
        
    }
}