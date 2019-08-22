using Sisfarma.ClickOnce;
using System;
using System.Linq;
using System.Threading.Tasks;
using ST = Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class UpdateVersionSincronizador : ST.Sincronizador
    {                
        public override void Process()
        {
            if (Updater.CheckUpdateSyncWithInfo())
            {
                var token = Updater.GetTokenSource();

                try
                {
                    token.Cancel();
                }
                catch (AggregateException ex)
                    when (ex.InnerExceptions.Any(inner => inner is TaskCanceledException))
                {
                    var canceledTasks = ex.InnerExceptions
                        .Where(inner => inner is TaskCanceledException)
                        .Select(x => ((TaskCanceledException)x).Task);

                    foreach (var t in canceledTasks)
                        t.Dispose();
                }
                finally
                {
                    token.Dispose();
                }

                Updater.UpdateHot();
                AppProcessHelper.BeginReStart();                
            }
        }
        
    }
}
