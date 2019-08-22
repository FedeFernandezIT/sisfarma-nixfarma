using System.Collections.Generic;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;

namespace Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia
{
    public interface ISinonimosRepository
    {
        IEnumerable<Sinonimo> GetAll();
    }
}
