using Sisfarma.Sincronizador.Core.Config;

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