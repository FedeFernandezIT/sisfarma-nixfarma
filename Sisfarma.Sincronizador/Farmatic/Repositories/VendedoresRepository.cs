using Sisfarma.Sincronizador.Config;
using Sisfarma.Sincronizador.Farmatic.Models;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;

namespace Sisfarma.Sincronizador.Farmatic.Repositories
{
    public class VendedoresRepository : FarmaticRepository
    {
        public VendedoresRepository(FarmaticContext ctx) : base(ctx)
        { }

        public VendedoresRepository(LocalConfig config) : base(config)
        { }

        public Vendedor GetOneOrDefaultById(short? idVendedor)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM vendedor WHERE IdVendedor = @idVendedor";
                return db.Database.SqlQuery<Vendedor>(sql,
                    new SqlParameter("idVendedor", idVendedor ?? SqlInt16.Null))
                    .FirstOrDefault();
            }
        }
    }
}