using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Fisiotes.DTO;
using Sisfarma.Sincronizador.Fisiotes.DTO.PuntosPendientes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Fisiotes.Repositories
{
    public class PuntosPendientesRepository : FisiotesRepository
    {
        public PuntosPendientesRepository(FisiotesContext ctx) : base(ctx)
        {
        }

        public PuntosPendientesRepository(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        {
        }

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

        public void CheckAndCreateFields()
        {
            const string table = @"SELECT * from pendiente_puntos LIMIT 0,1;";
            var fields = new[] { "dtoLinea", "dtoVenta", "proveedor", "redencion", "recetaPendiente" };
            var alters = new[]
            {
                @"ALTER TABLE pendiente_puntos ADD dtoLinea FLOAT DEFAULT 0;",
                @"ALTER TABLE pendiente_puntos ADD dtoVenta FLOAT DEFAULT 0;",
                @"ALTER TABLE pendiente_puntos ADD proveedor VARCHAR(255) DEFAULT NULL AFTER laboratorio;",
                @"ALTER TABLE pendiente_puntos ADD redencion FLOAT DEFAULT NULL;",
                @"ALTER TABLE pendiente_puntos ADD recetaPendiente CHAR(2) DEFAULT NULL;"
            };
            CheckAndCreateFieldsTemplate(table, fields, alters);
        }

        public void CheckTipoPagoField(string remote)
        {
            var sql =
                @"SELECT data_type AS tipo From information_schema.Columns WHERE TABLE_SCHEMA = @baseRemoto AND TABLE_NAME = 'pendiente_puntos' AND COLUMN_NAME = 'tipoPago'";
            var result = _ctx.Database.SqlQuery<string>(sql,
                new SqlParameter("baseRemoto", remote))
                .ToList();
            if (result.Count != 0)
            {
                if (result[0].Equals("char", StringComparison.CurrentCultureIgnoreCase))
                {
                    sql = @"ALTER TABLE pendiente_puntos MODIFY COLUMN tipoPago CHAR(2);";
                    _ctx.Database.ExecuteSqlCommand(sql);
                }
            }
            else
            {
                sql = @"ALTER TABLE pendiente_puntos ADD tipoPago CHAR(2) DEFAULT NULL AFTER precio;";
                _ctx.Database.ExecuteSqlCommand(sql);
            }
        }

        #region SQL Methods

        public IEnumerable<PuntosPendientes> GetOfRecetasPendientesSql()
        {
            var sql = @"SELECT * FROM pendiente_puntos WHERE (recetaPendiente IS NULL OR recetaPendiente = 'D') " +
                        @"AND YEAR(fechaVenta) >= 2016 GROUP BY idventa, idnlinea ORDER BY idventa ASC LIMIT 0,1000";
            return _ctx.Database.SqlQuery<PuntosPendientes>(sql).ToList();
        }

        public void UpdateSql(long venta, long linea)
        {
            var sql = @"UPDATE IGNORE pendiente_puntos SET recetaPendiente = 'C' WHERE IdVenta = @venta AND Idnlinea = @linea";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("venta", venta),
                new SqlParameter("linea", linea));
        }

        public void UpdateSql(string receta, long venta, long linea)
        {
            var sql = @"UPDATE IGNORE pendiente_puntos SET recetaPendiente = @receta WHERE IdVenta = @venta AND Idnlinea = @linea";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("receta", receta),
                new SqlParameter("venta", venta),
                new SqlParameter("linea", linea));
        }

        public void UpdateSql(string tipoPago, string proveedor, float? dtoLinea, float? dtoVenta, float redencion, long venta, long linea)
        {
            var sql =
                @"UPDATE IGNORE pendiente_puntos SET tipoPago = @tipoPago, proveedor = @proveedor, dtoLinea = @dtoLinea, dtoVenta = @dtoVenta, redencion = @redencion " +
                @"WHERE IdVenta = @venta AND Idnlinea= @linea";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("tipoPago", tipoPago),
                new SqlParameter("proveedor", proveedor),
                new SqlParameter("dtoLinea", dtoLinea),
                new SqlParameter("dtoVenta", dtoVenta),
                new SqlParameter("redencion", redencion),
                new SqlParameter("venta", venta),
                new SqlParameter("linea", linea));
        }

        public void UpdateSql(long venta)
        {
            var sql = @"UPDATE IGNORE pendiente_puntos SET tipoPago = 'C', redencion = 0 WHERE IdVenta = @venta";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("venta", venta));
        }

        public IEnumerable<PuntosPendientes> GetWithoutRedencionSql()
        {
            var sql =
                @"SELECT * FROM pendiente_puntos WHERE redencion IS NULL AND YEAR(fechaVenta) >= 2015 GROUP BY idventa ORDER BY idventa ASC LIMIT 0,1000";
            return _ctx.Database.SqlQuery<PuntosPendientes>(sql)
                .ToList();
        }

        public PuntosPendientes LastSql()
        {
            var sql = @"SELECT * FROM pendiente_puntos ORDER BY idventa DESC LIMIT 0,1";
            return _ctx.Database.SqlQuery<PuntosPendientes>(sql)
                .FirstOrDefault();
        }

        public PuntosPendientes GetByItemVentaSql(int venta, int linea)
        {
            var sql = @"SELECT * FROM pendiente_puntos WHERE IdVenta = @venta AND Idnlinea = @linea";
            return _ctx.Database.SqlQuery<PuntosPendientes>(sql,
                new SqlParameter("venta", venta),
                new SqlParameter("linea", linea))
                .FirstOrDefault();
        }

        public void InsertSql(int venta, int linea, string codigoBarra, string codigo, string descripcion, string familia, int cantidad, decimal numero,
                string tipoPago, int fecha, string dni, string cargado, string puesto, string trabajador, string codLaboratorio, string laboratorio, string proveedor,
                string receta, DateTime fechaVenta, string superFamlia, float precioMed, float pcoste, float dtoLinea, float dtoVta, float redencion, string recetaPendiente)
        {
            var sql = "INSERT IGNORE INTO pendiente_puntos " +
                    @"(idventa,idnlinea,cod_barras,cod_nacional,descripcion,familia,cantidad,precio,tipoPago,fecha,dni,cargado,puesto,trabajador," +
                    @"cod_laboratorio,laboratorio,proveedor,receta,fechaVenta,superFamilia, pvp, puc, dtoLinea, dtoVenta, redencion, recetaPendiente) " +
                    @"VALUES(" +
                    @"@venta, @linea, @codigoBarra, @codigo, @descripcion, @familia, @cantidad, @numero, @tipoPago, @fecha, @dni, @cargado, @puesto, @trabajador, " +
                    @"@codLaboratorio, @laboratorio, @proveedor, @receta, @fechaVenta, @superfamilia, @precioMed, @pcoste, @dtoLinea, @dtoVta, @redencion, @recetaPendiente)";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("venta", venta),
                new SqlParameter("linea", linea),
                new SqlParameter("codigoBarra", codigoBarra),
                new SqlParameter("codigo", codigo),
                new SqlParameter("descripcion", descripcion),
                new SqlParameter("familia", familia),
                new SqlParameter("cantidad", cantidad),
                new SqlParameter("numero", numero),
                new SqlParameter("tipoPago", tipoPago),
                new SqlParameter("fecha", fecha),
                new SqlParameter("dni", dni),
                new SqlParameter("cargado", cargado),
                new SqlParameter("puesto", puesto),
                new SqlParameter("trabajador", trabajador),
                new SqlParameter("codLaboratorio", codLaboratorio),
                new SqlParameter("laboratorio", laboratorio),
                new SqlParameter("proveedor", proveedor),
                new SqlParameter("receta", receta),
                new SqlParameter("fechaVenta", fechaVenta),
                new SqlParameter("superFamilia", superFamlia),
                new SqlParameter("precioMed", precioMed),
                new SqlParameter("pcoste", pcoste),
                new SqlParameter("dtoLinea", dtoLinea),
                new SqlParameter("dtoVta", dtoVta),
                new SqlParameter("redencion", redencion),
                new SqlParameter("recetaPendiente", recetaPendiente));
        }

        #endregion SQL Methods
    }
}