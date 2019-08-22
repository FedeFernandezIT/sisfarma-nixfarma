using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface ICategoriasExternalService
    {
        bool Exists(string categoria, string padre);

        Categoria GetByCategoriaAndPadreOrDefault(string categoria, string padre);

        Categoria GetByPadreOrDefault(string padre);

        void Sincronizar(Categoria cc);
    }
}