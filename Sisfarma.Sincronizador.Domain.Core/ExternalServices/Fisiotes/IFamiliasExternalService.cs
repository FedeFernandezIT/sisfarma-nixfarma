using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IFamiliasExternalService : IFamiliasExternalServiceNew
    {
        bool Exists(string familia);
        Familia GetByFamilia(string familia);
        decimal GetPuntosByFamiliaTipoVerificado(string familia);
        void Insert(Familia ff);
    }

    public interface IFamiliasExternalServiceNew
    {
        void Sincronizar(IEnumerable<Familia> familias);        
    }
}