using Sisfarma.Sincronizador.Config;
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
    public class EncargosRepository : FarmaticRepository
    {
        public EncargosRepository(FarmaticContext ctx) : base(ctx)
        { }

        public EncargosRepository(LocalConfig config) : base(config)
        { }

        public IEnumerable<Encargo> GetAllByContadorGreaterOrEqual(int year, long? contador)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT TOP 1000 * From Encargo WHERE year(idFecha) >= @year AND IdContador >= @contador Order by IdContador ASC";
                return db.Database.SqlQuery<Encargo>(sql,
                    new SqlParameter("year", year),
                    new SqlParameter("contador", contador ?? SqlInt64.Null))
                    .ToList();
            }
        }

        public IEnumerable<Encargo> GetAllGreaterOrEqualByFecha(DateTime fecha)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * From Encargo WHERE idFecha >= @fecha AND estado > 0 Order by idFecha DESC";
                return db.Database.SqlQuery<Encargo>(sql,
                    new SqlParameter("fecha", fecha))
                    .ToList();
            }
        }
    }
}