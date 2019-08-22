using System.Collections.Generic;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface ISinonimosExternalService
    {
        void Empty();
        void Sincronizar(List<Sinonimo> items);
        bool IsEmpty();
    }
}