using Sisfarma.Sincronizador.Domain.Entities.Farmacia;

namespace Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia
{
    public interface IVendedoresRepository
    {
        Vendedor GetOneOrDefaultById(long idVendedor);
    }
}