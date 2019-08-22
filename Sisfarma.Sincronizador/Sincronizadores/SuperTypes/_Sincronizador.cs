using System;
using System.Threading;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Sincronizadores.SuperTypes
{
    public abstract class Sincronizador : ISincronizadorAsync
    {
        protected const int DelayLoopDefault = 200;

        protected CancellationToken _cancellationToken;

        public abstract void Process();

        public virtual async Task SincronizarAsync(CancellationToken cancellationToken, int delayLoop = DelayLoopDefault)
        {
            _cancellationToken = cancellationToken;
            while (true)
            {
                try
                {
                    _cancellationToken.ThrowIfCancellationRequested();
                    Process();
                }
                catch (Exception)
                {
                    // nothing
                }
                finally
                {
                    await Task.Delay(delayLoop);
                }
            }
        }
    }
}