using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia
{
    public interface IProveedorRepository
    {
        IEnumerable<Proveedor> GetAll();

        Proveedor GetOneOrDefaultById(long id);

        Proveedor GetOneOrDefaultByCodigoNacional(string codigoNacional);
    }
}