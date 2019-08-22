using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia
{
    public class VendedoresRepository : FarmaciaRepository, IVendedoresRepository 
    {
        public VendedoresRepository(LocalConfig config) : base(config)
        { }

        public VendedoresRepository() { }

        public Vendedor GetOneOrDefaultById(long id)
        {
            var idInteger = (int)id;
            using (var db = FarmaciaContext.Vendedor())
            {
                var sql = @"SELECT Nombre FROM vendedores WHERE ID_Vendedor = @id";
                return db.Database.SqlQuery<Vendedor>(sql,
                    new OleDbParameter("id", idInteger))
                        .FirstOrDefault();
            }
        }
    }
}