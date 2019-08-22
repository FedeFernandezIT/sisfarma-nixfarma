using Sisfarma.Sincronizador.Config;
using Sisfarma.Sincronizador.Farmatic.DTO.Recepciones;
using Sisfarma.Sincronizador.Farmatic.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Farmatic.Repositories
{
    public class RecepcionesRepository : FarmaticRepository
    {
        public RecepcionesRepository(FarmaticContext ctx) : base(ctx)
        { }

        public RecepcionesRepository(LocalConfig config) : base(config)
        { }

        public IEnumerable<Recepcion> GetByYear(int year)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT TOP 1000 * From Recep WHERE YEAR(Fecha) >= @year Order by IdRecepcion ASC";
                return db.Database.SqlQuery<Recepcion>(sql,
                    new SqlParameter("year", year))
                    .ToList();
            }
        }

        public IEnumerable<Recepcion> GetByIdAndYear(int year, long? pedido)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql =
                @"SELECT TOP 1000 * From Recep WHERE IdRecepcion >= @pedido AND YEAR(Fecha) >= @year Order by IdRecepcion ASC";
                return db.Database.SqlQuery<Recepcion>(sql,
                    new SqlParameter("year", year),
                    new SqlParameter("pedido", pedido ?? SqlInt64.Null))
                    .ToList();
            }
        }

        public IEnumerable<RecepcionGroup> GetGroupGreaterThanByFecha(DateTime fecha)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT lr.XArt_IdArticu, r.XProv_IdProveedor, r.hora, lr.ImportePuc FROM Recep r " +
                        @"INNER JOIN LineaRecep lr ON lr.IdRecepcion = r.IdRecepcion " +
                        @"WHERE r.hora > @fecha " +
                        @"GROUP BY lr.XArt_IdArticu, r.XProv_IdProveedor, r.hora, lr.ImportePuc " +
                        @"ORDER BY r.hora DESC";
                return db.Database.SqlQuery<RecepcionGroup>(sql,
                    new SqlParameter("fecha", fecha))
                    .ToList();
            }
        }

        internal IEnumerable<RecepcionGroup> GetGroupGreaterOrEqualByFecha(DateTime fecha)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT lr.XArt_IdArticu, r.XProv_IdProveedor, r.hora, lr.ImportePuc FROM Recep r " +
                        @"INNER JOIN LineaRecep lr ON lr.IdRecepcion = r.IdRecepcion " +
                        @"WHERE r.hora >= @fecha " +
                        @"GROUP BY lr.XArt_IdArticu, r.XProv_IdProveedor, r.hora, lr.ImportePuc " +
                        @"ORDER BY r.hora DESC";
                return db.Database.SqlQuery<RecepcionGroup>(sql,
                    new SqlParameter("fecha", fecha))
                    .ToList();
            }
        }

        public RecepcionResume GetResumeById(int recepcion)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT ISNULL(COUNT(IdNLinea),0) AS numLineas, ISNULL(SUM(recibidas*ImportePvp),0) AS importePvp, ISNULL(SUM(importe),0) AS importePuc " +
                @"FROM LINEARECEP WHERE IdRecepcion = @recepcion AND (Recibidas <> 0 OR UDevolver <> 0)";
                return db.Database.SqlQuery<RecepcionResume>(sql,
                    new SqlParameter("recepcion", recepcion))
                    .Single();
            }
        }

        public IEnumerable<LineaRecepcion> GetLineasById(int recepcion)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"select * from LINEARECEP where IdRecepcion = @recepcion AND (Recibidas <> 0 OR UDevolver <> 0)";
                return db.Database.SqlQuery<LineaRecepcion>(sql,
                    new SqlParameter("recepcion", recepcion))
                    .ToList();
            }
        }
    }
}