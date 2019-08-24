using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Linq;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public class CategoriasRepository : FarmaciaRepository, ICategoriasRepository
    {        
        public CategoriasRepository(LocalConfig config) : base(config)
        { }

        public CategoriasRepository() { }
        

        public IEnumerable<Categoria> GetAll()
        {
            using (var db = FarmaciaContext.Default())
            {
                var sql = "select Nombre from categorias";
                return db.Database.SqlQuery<Categoria>(sql)
                    .ToList();
            }
        }

        public IEnumerable<Categoria> GetAllByDescripcion()
        {
            var rs = new List<Categoria>();
            using (var db = FarmaciaContext.Default())
            {
                var sql = @"select IDCategoria as Id, Nombre from categorias WHERE nombre NOT IN ('ESPECIALIDAD', 'EFP', 'SIN FAMILIA') AND nombre NOT LIKE '%ESPECIALIDADES%' AND nombre NOT LIKE '%Medicamento%'";
                rs = db.Database.SqlQuery<DTO.Categoria>(sql)
                    .Select(x => new Categoria { Id = x.Id, Nombre = x.Nombre})
                        .ToList();
            }

            rs.ForEach(item => 
                item.Subcategorias = GetAllNombreSubcategoriaByCategoriaId(item.Id));            
            return rs;
        }

        public IEnumerable<string> GetAllNombreSubcategoriaByCategoriaId(long id)
        {
            using (var db = FarmaciaContext.Default())
            {
                var sql = @"SELECT nombre FROM Subcategorias WHERE  IdCategoria = @id";
                return db.Database.SqlQuery<string>(sql,
                    new OleDbParameter("id", (int) id))
                    .ToList();
            }
        }

        public string GetSubCategoriaById(string id)
        {
            using (var db = FarmaciaContext.Default())
            {
                var sql = @"SELECT nombre FROM Subcategorias WHERE  IdCategoria = @id";
                return db.Database.SqlQuery<string>(sql,
                    new OleDbParameter("id", int.Parse(id)))
                    .FirstOrDefault();
            }
        }
    }
}