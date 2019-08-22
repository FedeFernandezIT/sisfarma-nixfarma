using System.Threading;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes
{
    public interface ISincronizadorAsync
    {
        Task SincronizarAsync(CancellationToken cancellationToken, int delayLoop);
    }
}