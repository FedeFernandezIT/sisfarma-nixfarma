using System.Threading;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Sincronizadores.SuperTypes
{
    public interface ISincronizadorAsync
    {
        Task SincronizarAsync(CancellationToken cancellationToken, int delayLoop);
    }
}