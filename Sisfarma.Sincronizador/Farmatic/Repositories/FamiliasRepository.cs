using Sisfarma.Sincronizador.Config;
using Sisfarma.Sincronizador.Farmatic.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Farmatic.Repositories
{
    public class FamiliasRepository : FarmaticRepository
    {
        public FamiliasRepository(FarmaticContext ctx) : base(ctx)
        { }

        public FamiliasRepository(LocalConfig config) : base(config)
        { }

        public Familia GetById(short id)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM familia WHERE IdFamilia = @id";
                return db.Database.SqlQuery<Familia>(sql,
                        new SqlParameter("id", id))
                    .FirstOrDefault();
            }
        }

        public IEnumerable<Familia> Get()
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = "select * from familia";
                return db.Database.SqlQuery<Familia>(sql)
                    .ToList();
            }
        }

        public List<Familia> GetByDescripcion()
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"select * from familia WHERE descripcion NOT IN ('ESPECIALIDAD', 'EFP', 'SIN FAMILIA') AND descripcion NOT LIKE '%ESPECIALIDADES%' AND descripcion NOT LIKE '%Medicamento%'";
                return db.Database.SqlQuery<Familia>(sql)
                    .ToList();
            }
        }

        public string GetSuperFamiliaDescripcionByFamilia(string familia)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql =
                @"SELECT sf.Descripcion FROM SuperFamilia sf INNER JOIN FamiliaAux fa ON fa.IdSuperFamilia = sf.IdSuperFamilia " +
                @" INNER JOIN Familia f ON f.IdFamilia = fa.IdFamilia WHERE f.Descripcion = @familia";
                return db.Database.SqlQuery<string>(sql,
                    new SqlParameter("familia", familia))
                    .FirstOrDefault();
            }
        }

        public string GetSuperFamiliaDescripcionById(short familia)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql =
                @"SELECT sf.Descripcion FROM SuperFamilia sf INNER JOIN FamiliaAux fa ON sf.IdSuperFamilia = fa.IdSuperFamilia " +
                @"WHERE fa.IdFamilia = @familia";
                return db.Database.SqlQuery<string>(sql,
                    new SqlParameter("familia", familia))
                    .FirstOrDefault();
            }
        }
    }
}