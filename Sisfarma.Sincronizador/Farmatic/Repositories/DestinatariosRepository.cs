using Sisfarma.Sincronizador.Config;
using Sisfarma.Sincronizador.Farmatic.Models;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Farmatic.Repositories
{
    public class DestinatariosRepository : FarmaticRepository
    {
        public DestinatariosRepository(FarmaticContext ctx) : base(ctx)
        { }

        public DestinatariosRepository(LocalConfig config) : base(config)
        { }

        public List<Destinatario> GetByCliente(string cliente)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM Destinatario WHERE fk_Cliente_1 = @idCliente";
                return db.Database.SqlQuery<Destinatario>(sql,
                    new SqlParameter("idCliente", cliente))
                    .ToList();
            }
        }
    }
}