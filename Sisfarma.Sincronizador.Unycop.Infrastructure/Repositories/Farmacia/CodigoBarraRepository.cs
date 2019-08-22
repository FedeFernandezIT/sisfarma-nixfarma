using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia
{
    public interface ICodigoBarraRepository
    {
        string GetOneByFarmacoId(long farmaco);
    }


    public class CodigoBarraRepository : FarmaciaRepository, ICodigoBarraRepository
    {
        public CodigoBarraRepository(LocalConfig config) : base(config)
        { }

        public CodigoBarraRepository() { }

        public string GetOneByFarmacoId(long farmaco)
        {
            var farmacoInteger = (int)farmaco;
            using (var db = FarmaciaContext.Default())
            {
                var sql = @"select Cod_Barra from codigo_barra where ID_farmaco = @farmaco";                    
                return db.Database.SqlQuery<string>(sql,
                    new OleDbParameter("farmaco", farmacoInteger))
                        .FirstOrDefault();
            }
        }
    }
}
