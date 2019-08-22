using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Farmatic.Models;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Data;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Farmatic.Repositories
{
    public class DestinatariosRepository : FarmaciaRepository
    {        
        public DestinatariosRepository(LocalConfig config) : base(config)
        { }

        public List<Destinatario> GetByCliente(string cliente)
        {
            using (var db = FarmaciaContext.Create(_config))
            {
                var sql = @"SELECT * FROM Destinatario WHERE fk_Cliente_1 = @idCliente";
                return db.Database.SqlQuery<Destinatario>(sql,
                    new SqlParameter("idCliente", cliente))
                    .ToList();
            }
        }
    }
}