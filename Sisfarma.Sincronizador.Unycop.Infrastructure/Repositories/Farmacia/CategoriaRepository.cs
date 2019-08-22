using System.Data.OleDb;
using System.Linq;
using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Data;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia
{
    public interface ICategoriaRepository
    {
        Categoria GetOneOrDefaultById(long id);

        Subcategoria GetSubcategoriaOneOrDefaultByKey(long categoria, long id);
    }

    public class CategoriaRepository : FarmaciaRepository, ICategoriaRepository
    {
        public CategoriaRepository(LocalConfig config) : base(config)
        { }

        public CategoriaRepository()
        { }

        public Categoria GetOneOrDefaultById(long id)
        {
            var idInteger = (int)id;
            using (var db = FarmaciaContext.Default())
            {
                var sql = @"SELECT Nombre FROM categorias WHERE IDCategoria = @id";
                return db.Database.SqlQuery<Categoria>(sql,
                    new OleDbParameter("id", idInteger))
                    .FirstOrDefault();
            }
                
        }

        public Subcategoria GetSubcategoriaOneOrDefaultByKey(long categoria, long id)
        {
            var categInteger = (int)categoria;
            var idInteger = (int)id;

            using (var db = FarmaciaContext.Default())
            {
                var sql = "SELECT Nombre FROM subcategorias WHERE IdSubCategoria = @id AND IdCategoria = @categoria";
                return db.Database.SqlQuery<Subcategoria>(sql,
                    new OleDbParameter("id", idInteger),
                    new OleDbParameter("categoria", categInteger))
                        .FirstOrDefault();
            }
        }
    }
}
