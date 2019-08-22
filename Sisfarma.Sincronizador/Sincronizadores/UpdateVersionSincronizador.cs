using Sisfarma.Sincronizador.Helpers;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using ST = Sisfarma.Sincronizador.Sincronizadores.SuperTypes;

namespace Sisfarma.Sincronizador.Sincronizadores
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
                //Application.ExitThread();
                //Application.Restart();
                //Application.Exit();                           
            }
        }
        
    }
}
