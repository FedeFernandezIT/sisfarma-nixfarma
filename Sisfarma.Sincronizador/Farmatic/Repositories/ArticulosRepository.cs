using Sisfarma.Sincronizador.Config;
using Sisfarma.Sincronizador.Farmatic.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;

namespace Sisfarma.Sincronizador.Farmatic.Repositories
{
    public class ArticulosRepository : FarmaticRepository
    {
        public ArticulosRepository(FarmaticContext ctx) : base(ctx)
        { }

        public ArticulosRepository(LocalConfig config) : base(config)
        { }

        public bool Exists(string codigo) => this.GetOneOrDefaultById(codigo) != null;

        public Articulo GetOneOrDefaultById(string codigo)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM articu WHERE IdArticu = @codigo";
                return db.Database.SqlQuery<Articulo>(sql,
                    new SqlParameter("codigo", codigo))
                    .FirstOrDefault();
            }
        }

        public List<ArticuloWithIva> GetWithoutStockByIdGreaterOrEqual(string codArticulo)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"select top 1000 a.*, t.Piva AS iva from articu a INNER JOIN Tablaiva t ON t.IdTipoArt = a.XGrup_IdGrupoIva AND t.IdTipoPro = '05' " +
                @" WHERE a.Descripcion <> 'PENDIENTE DE ASIGNACIÓN' AND a.Descripcion <> 'VENTAS VARIAS' AND a.Descripcion <> '   BASE DE DATOS  3/03/2014' " +
                @" AND a.IdArticu >= @codArticulo AND a.StockActual <= 0 ORDER BY a.IdArticu ASC";
                return db.Database.SqlQuery<ArticuloWithIva>(sql,
                    new SqlParameter("codArticulo", codArticulo))
                    .ToList();
            }
        }

        public List<ArticuloWithIva> GetWithStockByIdGreaterOrEqual(string codArticulo)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"select top 1000 a.*, t.Piva AS iva from articu a INNER JOIN Tablaiva t ON t.IdTipoArt = a.XGrup_IdGrupoIva AND t.IdTipoPro = '05' " +
                @" WHERE a.Descripcion <> 'PENDIENTE DE ASIGNACIÓN' AND a.Descripcion <> 'VENTAS VARIAS' AND a.Descripcion <> '   BASE DE DATOS  3/03/2014' " +
                @" AND a.IdArticu >= @codArticulo AND a.StockActual > 0 ORDER BY a.IdArticu ASC";
                return db.Database.SqlQuery<ArticuloWithIva>(sql,
                    new SqlParameter("codArticulo", codArticulo))
                    .ToList();
            }
        }

        public List<ArticuloWithIva> GetByFechaUltimaSalidaGreaterOrEqual(DateTime? fechaActualizacionStock)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"select a.*, t.Piva AS iva from articu a INNER JOIN Tablaiva t ON t.IdTipoArt = a.XGrup_IdGrupoIva AND t.IdTipoPro = '05' " +
                @"WHERE a.Descripcion <> 'PENDIENTE DE ASIGNACIÓN' AND a.Descripcion <> 'VENTAS VARIAS' AND a.Descripcion <> '   BASE DE DATOS  3/03/2014' " +
                @"AND FechaUltimaSalida >= @fechaActualizacion ORDER BY FechaUltimaSalida ASC";
                return db.Database.SqlQuery<ArticuloWithIva>(sql,
                    new SqlParameter("fechaActualizacion", fechaActualizacionStock ?? SqlDateTime.Null))
                    .ToList();
            }
        }

        public ArticuloWithIva GetControlArticuloFisrtOrDefault(string articulo)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"select TOP 1 idArticu from articu " +
                  " WHERE Descripcion <> 'PENDIENTE DE ASIGNACIÓN' AND Descripcion <> 'VENTAS VARIAS' AND Descripcion <> '   BASE DE DATOS  3/03/2014' " +
                  " AND IdArticu > @articulo AND StockActual > 0 ORDER BY IdArticu ASC";

                return db.Database.SqlQuery<ArticuloWithIva>(sql,
                    new SqlParameter("articulo", articulo))
                    .FirstOrDefault();
            }
        }

        public ArticuloWithIva GetControlArticuloSinStockFisrtOrDefault(string articulo)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"select TOP 1 idArticu from articu " +
                  " WHERE Descripcion <> 'PENDIENTE DE ASIGNACIÓN' AND Descripcion <> 'VENTAS VARIAS' AND Descripcion <> '   BASE DE DATOS  3/03/2014' " +
                  " AND IdArticu > @articulo AND StockActual <= 0 ORDER BY IdArticu ASC";

                return db.Database.SqlQuery<ArticuloWithIva>(sql,
                    new SqlParameter("articulo", articulo))
                    .FirstOrDefault();
            }
        }

        public List<ArticuloWithIva> GetByFechaUltimaEntradaGreaterOrEqual(DateTime? fechaActualizacionStock)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"select top 1000 a.*, t.Piva AS iva from articu a INNER JOIN Tablaiva t ON t.IdTipoArt = a.XGrup_IdGrupoIva AND t.IdTipoPro = '05' " +
                @"WHERE a.Descripcion <> 'PENDIENTE DE ASIGNACIÓN' AND a.Descripcion <> 'VENTAS VARIAS' AND a.Descripcion <> '   BASE DE DATOS  3/03/2014' " +
                @"AND FechaUltimaEntrada >= @fechaActualizacion ORDER BY FechaUltimaEntrada ASC";
                return db.Database.SqlQuery<ArticuloWithIva>(sql,
                    new SqlParameter("fechaActualizacion", fechaActualizacionStock ?? SqlDateTime.Null))
                    .ToList();
            }
        }
    }
}