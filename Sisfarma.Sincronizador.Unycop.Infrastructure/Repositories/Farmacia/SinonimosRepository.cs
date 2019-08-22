using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Data;
using System.Collections.Generic;
using System.Linq;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia
{
    public class SinonimosRepository : FarmaciaRepository, ISinonimosRepository
    {        
        public SinonimosRepository(LocalConfig config) : base(config)
        { }

        public SinonimosRepository() { }

        public IEnumerable<Sinonimo> GetAll()
        {
            using (var db = FarmaciaContext.Default())
            {
                var sql = @"SELECT Cod_Barra as Serial, ID_Farmaco as Farmaco FROM codigo_barra";
                return db.Database.SqlQuery<DTO.CodigoBarra>(sql)
                    //.Take(1000)
                    .ToList()
                    .Select(x => new Sinonimo { CodigoBarra = x.Serial, CodigoNacional = x.Farmaco.ToString()});
            }
        }        
    }
}