using Sisfarma.Sincronizador.Config;
using Sisfarma.Sincronizador.Farmatic.Models;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Farmatic.Repositories
{
    public class LaboratoriosRepository : FarmaticRepository
    {
        public LaboratoriosRepository(FarmaticContext ctx) : base(ctx)
        { }

        public LaboratoriosRepository(LocalConfig config) : base(config)
        { }

        public Laboratorio GetById(string codigo)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM laboratorio WHERE codigo = @codigo";
                return db.Database.SqlQuery<Laboratorio>(sql,
                    new SqlParameter("codigo", codigo))
                    .FirstOrDefault();
            }
        }
    }
}