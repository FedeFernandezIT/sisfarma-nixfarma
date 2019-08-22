using System.Collections.Generic;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IConfiguracionesExternalService
    {
        string GetByCampo(string field);
        IEnumerable<Configuracion> GetEstadosActuales();
        bool PerteneceFarmazul();
        void Update(string field, string value);
    }
}