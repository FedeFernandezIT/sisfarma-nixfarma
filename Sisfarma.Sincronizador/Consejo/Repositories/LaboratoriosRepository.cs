using Sisfarma.Sincronizador.Consejo.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Consejo.Repositories
{
    public class LaboratoriosRepository : ConsejoRepository
    {
        public LaboratoriosRepository(ConsejoContext ctx) : base(ctx)
        {
        }
        public Labor Get(string codigo)
        {
            var sql = @"SELECT * FROM LABOR WHERE CODIGO = @codigo";
            return _ctx.Database.SqlQuery<Labor>(sql,
                new SqlParameter("codigo", codigo))
                .FirstOrDefault();
        }
    }
}
