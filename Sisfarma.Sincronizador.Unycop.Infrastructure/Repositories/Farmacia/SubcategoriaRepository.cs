using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Data;
using System.Collections.Generic;
using System.Linq;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia
{
    public class SubcategoriaRepository : FarmaciaRepository, ISubcategoriasRepository
    {
        public IEnumerable<Subcategoria> GetAll()
        {
            using (var db = FarmaciaContext.Default())
            {
                var sql = "select Nombre from subcategorias";
                return db.Database.SqlQuery<Subcategoria>(sql)
                    .ToList();
            }
        }
    }
}
