using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IProgramacionExternalService
    {
        Programacion GetProgramacionOrDefault(string estado);
    }
}