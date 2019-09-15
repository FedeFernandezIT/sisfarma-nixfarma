namespace Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia
{
    public interface IFarmacosRepository
    {
        bool Exists(string codigo);
    }
}