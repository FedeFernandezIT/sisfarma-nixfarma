using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia
{
    public interface IProveedorRepository
    {
        IEnumerable<Proveedor> GetAll();

        Proveedor GetOneOrDefaultById(long id);

        Proveedor GetOneOrDefaultByCodigoNacional(long codigoNacional);
    }
}
