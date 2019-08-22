using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IHuecosExternalService
    {
        bool Any(int value);
        void Delete(string hueco);
        IEnumerable<string> GetByOrderAsc();
        void Insert(string[] huecos);
    }
}