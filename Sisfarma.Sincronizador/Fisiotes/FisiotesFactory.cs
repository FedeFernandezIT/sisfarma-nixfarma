using Sisfarma.Sincronizador.Config;

namespace Sisfarma.Sincronizador.Fisiotes
{
    public class FisiotesFactory
    {
        public static FisiotesService New() => new FisiotesService(RemoteConfig.GetSingletonInstance());
    }
}
