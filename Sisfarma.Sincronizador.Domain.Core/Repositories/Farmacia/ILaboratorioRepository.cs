using Sisfarma.Sincronizador.Domain.Entities.Farmacia;

namespace Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia
{
    public interface ILaboratorioRepository
    {
        Laboratorio GetOneOrDefaultByCodigo(long codigo, string clase, string claseBot);
    }
}