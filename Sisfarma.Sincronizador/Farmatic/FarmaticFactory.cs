using Sisfarma.Sincronizador.Config;

namespace Sisfarma.Sincronizador.Farmatic
{
    public class FarmaticFactory
    {
        public static FarmaticService New() => new FarmaticService(LocalConfig.GetSingletonInstance());
    }
}
