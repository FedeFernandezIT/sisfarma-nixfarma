using Sisfarma.Sincronizador.Config;
using Sisfarma.Sincronizador.Farmatic.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Farmatic.Repositories
{
    public class VentasRepository : FarmaticRepository
    {
        public VentasRepository(FarmaticContext ctx) : base(ctx)
        { }

        public VentasRepository(LocalConfig config) : base(config)
        { }

        public LineaVenta GetLineaVentaOrDefaultByKey(long venta, long linea)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM lineaventa WHERE IdVenta = @venta AND IdNLinea = @linea";
                return db.Database.SqlQuery<LineaVenta>(sql,
                    new SqlParameter("venta", venta),
                    new SqlParameter("linea", linea))
                    .FirstOrDefault();
            }
        }

        public List<Venta> GetByIdGreaterOrEqual(int year, long value)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT TOP 1000 * FROM venta WHERE ejercicio >= @year AND IdVenta >= @value ORDER BY IdVenta ASC";
                return db.Database.SqlQuery<Venta>(sql,
                    new SqlParameter("year", year),
                    new SqlParameter("value", value))
                    .ToList();
            }
        }

        public List<Venta> GetVirtualesLessThanId(long venta)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT v.* FROM venta v INNER JOIN lineaventavirtual lvv ON lvv.idventa = v.idventa AND (lvv.codigo = 'Pago' OR lvv.codigo = 'A Cuenta') " +
                    @"WHERE v.ejercicio >= 2015 AND v.IdVenta < @venta ORDER BY v.IdVenta DESC";
                return db.Database.SqlQuery<Venta>(sql,
                    new SqlParameter("venta", venta))
                    .ToList();
            }
        }

        public List<LineaVentaVirtual> GetLineasVirtualesByVenta(int venta)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql =
                @"SELECT * FROM lineaventavirtual WHERE IdVenta = @venta AND (codigo = 'Pago' OR codigo = 'A Cuenta')";
                return db.Database.SqlQuery<LineaVentaVirtual>(sql,
                    new SqlParameter("venta", venta))
                    .ToList();
            }
        }

        public Venta GetOneOrDefaultById(long venta)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM venta WHERE IdVenta = @venta ORDER BY IdVenta ASC";
                return db.Database.SqlQuery<Venta>(sql,
                    new SqlParameter("venta", venta))
                    .FirstOrDefault();
            }
        }

        public List<Venta> GetGreatThanOrEqual(long venta, DateTime fecha)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var year = fecha.Year;
                var sql = "SELECT * FROM venta WHERE IdVenta >= @venta AND  ejercicio = @year AND FechaHora >= @fecha ORDER BY IdVenta ASC";

                return db.Database.SqlQuery<Venta>(sql,
                    new SqlParameter("venta", venta),
                    new SqlParameter("year", year),
                    new SqlParameter("fecha", fecha))
                    .ToList();
            }
        }

        public List<LineaVenta> GetLineasVentaByVenta(int venta)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM lineaventa WHERE IdVenta = @idVenta";
                return db.Database.SqlQuery<LineaVenta>(sql,
                    new SqlParameter("idVenta", venta))
                    .ToList();
            }
        }

        public LineaVentaRedencion GetOneOrDefaultLineaRedencionByKey(int venta, int linea)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM LineaVentaReden WHERE IdVenta = @venta AND IdNLinea = @linea";
                return db.Database.SqlQuery<LineaVentaRedencion>(sql,
                        new SqlParameter("venta", venta),
                        new SqlParameter("linea", linea))
                    .FirstOrDefault();
            }
        }
    }
}