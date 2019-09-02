using System.Collections.Generic;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;

namespace Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia
{
    public interface IClientesRepository
    {
        bool EsBeBlue(string tipoCliente, string tipoDescuento);

        bool Exists(int id);

        T GetAuxiliarById<T>(string cliente) where T : ClienteAux;

        List<Cliente> GetGreatThanId(long id);

        Cliente GetOneOrDefaultById(long id, bool cargarPuntosSisfarma);

        decimal GetTotalPuntosById(string idCliente);

        bool HasSexoField();

        string EsResidencia(string tipo, string descuento, string filtros);
    }
}