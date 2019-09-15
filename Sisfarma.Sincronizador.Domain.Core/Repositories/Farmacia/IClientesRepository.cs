using System.Collections.Generic;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;

namespace Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia
{
    public interface IClientesRepository
    {
        bool EsBeBlue(string tipoCliente, string tipoDescuento);

        bool Exists(int id);

        Cliente GetOneOrDefaultById(long id, bool cargarPuntosSisfarma);

        string EsResidencia(string tipo, string descuento, string filtros);
    }
}