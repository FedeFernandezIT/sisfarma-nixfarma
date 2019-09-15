using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using System;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia
{
    public interface IRecepcionRepository
    {
        IEnumerable<Recepcion> GetAllByDate(DateTime fecha);

        IEnumerable<ProveedorHistorico> GetAllHistoricosByFecha(DateTime fechaMax);
    }
}