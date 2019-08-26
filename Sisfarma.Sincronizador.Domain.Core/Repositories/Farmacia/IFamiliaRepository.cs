using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia
{
    public interface IFamiliaRepository
    {
        IEnumerable<Familia> GetAll();

        IEnumerable<Familia> GetByDescripcion();

        Familia GetOneOrDefaultById(long id);

        string GetSuperFamiliaDescripcionByFamilia(string familia);

        string GetSuperFamiliaDescripcionById(string id);

        Familia GetSubFamiliaOneOrDefault(long familia, string subFamilia);
    }
}