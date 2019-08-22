using System;
using System.Collections.Generic;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IProveedoresExternalService
    {
        DateTime? GetFechaMaximaDeHistorico();
        Proveedor GetOneOrDefault(string proveedor, string nombre);
        void Sincronizar(Proveedor pp);
        void Sincronizador(IEnumerable<ProveedorHistorial> items);
        void Update(Proveedor pp);
    }
}