using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia
{
    public interface ICategoriasRepository
    {
        IEnumerable<Categoria> GetAll();
    }
}