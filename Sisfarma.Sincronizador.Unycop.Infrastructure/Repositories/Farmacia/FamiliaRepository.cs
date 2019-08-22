using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Data;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia
{
    public class FamiliaRepository : FarmaciaRepository, IFamiliaRepository
    {
        public FamiliaRepository(LocalConfig config) : base(config)
        { }

        public FamiliaRepository() { }

        public IEnumerable<Familia> GetAll()
        {
            using (var db = FarmaciaContext.Default())
            {
                var sql = @"select Nombre from familias";
                return db.Database.SqlQuery<Familia>(sql)                    
                    .ToList();
            }            
        }

        public IEnumerable<Familia> GetByDescripcion()
        {            
            using (var db = FarmaciaContext.Default())
            {
                var sql = @"select nombre from familias WHERE nombre NOT IN ('ESPECIALIDAD', 'EFP', 'SIN FAMILIA') AND nombre NOT LIKE '%ESPECIALIDADES%' AND nombre NOT LIKE '%Medicamento%'";
                return db.Database.SqlQuery<Familia>(sql)
                    .ToList();
            }
        }

        public Familia GetOneOrDefaultById(long id)
        {
            var idInteger = (int)id;

            using (var db = FarmaciaContext.Default())
            {
                var sql = "SELECT Nombre FROM familias WHERE ID_Familia = @id";
                return db.Database.SqlQuery<Familia>(sql,
                    new OleDbParameter("id", id))
                        .FirstOrDefault();
            }
        }

        public string GetSuperFamiliaDescripcionByFamilia(string familia)
        {
            throw new NotImplementedException();
        }

        public string GetSuperFamiliaDescripcionById(short familia)
        {
            throw new NotImplementedException();
        }
    }
}
