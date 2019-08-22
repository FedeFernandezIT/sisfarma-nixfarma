using Sisfarma.Sincronizador.Config;
using Sisfarma.Sincronizador.Core.Config;

namespace Sisfarma.Sincronizador.Domain.Core.Services
{
    public class FarmaciaFactory
    {
        public static FarmaciaService New() => new FarmaciaService(LocalConfig.GetSingletonInstance());
    }
}
