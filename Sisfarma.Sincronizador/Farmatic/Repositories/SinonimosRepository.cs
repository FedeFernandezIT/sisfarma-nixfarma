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
    public class SinonimosRepository : FarmaticRepository
    {
        public SinonimosRepository(FarmaticContext ctx) : base(ctx)
        { }

        public SinonimosRepository(LocalConfig config) : base(config)
        { }

        public List<Sinonimos> GetAll()
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM Sinonimo";
                return db.Database.SqlQuery<Sinonimos>(sql)
                    .ToList();
            }
        }

        public Sinonimos GetOneOrDefaultByArticulo(string codigo)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM sinonimo WHERE IdArticu = @codigo";
                return db.Database.SqlQuery<Sinonimos>(sql,
                    new SqlParameter("codigo", codigo))
                    .FirstOrDefault();
            }
        }
    }
}