using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public interface ICodigoBarraRepository
    {
    }

    public class CodigoBarraRepository : FarmaciaRepository, ICodigoBarraRepository
    {
        public CodigoBarraRepository(LocalConfig config) : base(config)
        { }

        public CodigoBarraRepository()
        {
        }
    }
}