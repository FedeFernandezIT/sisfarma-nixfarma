using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia
{
    public class LaboratorioRepository : FarmaciaRepository, ILaboratorioRepository
    {
        protected const string LABORATORIO_DEFAULT = "<Sin Laboratorio>";

        public LaboratorioRepository(LocalConfig config) : base(config)
        { }

        public LaboratorioRepository() { }

        public Laboratorio GetOneOrDefaultByCodigo(string codigo)
        {
            using (var db = FarmaciaContext.Default())
            {
                var sql = "SELECT ID_Laboratorio as Codigo, Nombre FROM Laboratorios WHERE ID_Laboratorio = @codigo";
                return db.Database.SqlQuery<Laboratorio>(sql,
                    new OleDbParameter("codigo", codigo))
                    .FirstOrDefault()
                        ?? new Laboratorio { Codigo = codigo, Nombre = LABORATORIO_DEFAULT };
            }
        }        
    }
}