using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public static class SincronizadorTaskManager
    {
        public static ConcurrentBag<Task> CurrentTasks;
        public static CancellationTokenSource TokenSource;
        public static List<KeyValuePair<TaskSincronizador, int>> TaskSincronizadores { get; set; } = new List<KeyValuePair<TaskSincronizador, int>>();

        // milisegundos
        //public static readonly int DelayCategoria = 3600000;
        //public static readonly int DelayFamilia = 300000;
        //public static readonly int DelayClientes = 30000;
        //public static readonly int DelayClientesHuecos = 300000;
        //public static readonly int DelayControlStock = 10000;
        //public static readonly int DelayControlStockFechas = 60000;
        //public static readonly int DelayEncargosActualizar = 60000;
        //public static readonly int DelayListas = 300000;
        //public static readonly int DelayEncargos = 60000;
        //public static readonly int DelayProductosCriticos = 60000;
        //public static readonly int DelayPedidos = 10000;
        //public static readonly int DelayPuntosPendiente = 5000;
        //public static readonly int DelayProductosBorrar = 60000;
        //public static readonly int DelayVentaMensual = 300000;
        //public static readonly int DelaySinomimos = 300000;
        //public static readonly int DelayProveedores = 3600000;
        //public static readonly int DelayProveedoresHistorico = 300000;
        //public static readonly int DelayRecetaPendiente = 60000;

        public static readonly int DelayCategoria = 3600000;
        public static readonly int DelayFamilia = 300000;
        public static readonly int DelayClientes = 30000;
        public static readonly int DelayClientesHuecos = 300000;
        public static readonly int DelayControlStock = 300000;
        public static readonly int DelayControlStockFechas = 3000000;
        public static readonly int DelayEncargosActualizar = 300000;
        public static readonly int DelayListas = 300000;
        public static readonly int DelayEncargos = 60000;
        public static readonly int DelayProductosCriticos = 60000;
        public static readonly int DelayPedidos = 60000;
        public static readonly int DelayPuntosPendiente = 5000;
        public static readonly int DelayProductosBorrar = 300000;
        public static readonly int DelayVentaMensual = 300000;
        public static readonly int DelaySinomimos = 300000;
        public static readonly int DelayProveedores = 3600000;
        public static readonly int DelayProveedoresHistorico = 300000;
        public static readonly int DelayRecetaPendiente = 60000;

        public static ConcurrentBag<Task> CreateConcurrentTasks()
        {
            DisposeTasks();

            TokenSource = new CancellationTokenSource();
            var cancellationToken = TokenSource.Token;

            //var listaDeCompra = LocalConfig.GetSingletonInstance().ListaDeCompras;

            var tasks = TaskSincronizadores.Select(t => RunTask(t.Key, cancellationToken, t.Value));

            CurrentTasks = new ConcurrentBag<Task>(tasks);

            return CurrentTasks;
        }

        public static List<KeyValuePair<T, int>> AddSincronizador<T>(this List<KeyValuePair<T, int>> @this,  T sincronizador, int delay) where T : TaskSincronizador
        {
            @this.Add(new KeyValuePair<T, int>(sincronizador, delay));
            return @this;
        }

        private static void DisposeTasks()
        {
            if (CurrentTasks == null)
                return;

            var tasks = CurrentTasks.ToArray();
            if (tasks.Any(t => t.Status == TaskStatus.Running))
            {
                TokenSource.Cancel();
                Task.WaitAll(tasks);
                foreach (var task in tasks)
                {
                    task.Dispose();
                }
            }
            TokenSource.Dispose();
            CurrentTasks = null;
        }

        public static void PowerOn()
        {
            CreateConcurrentTasks();
            Console.WriteLine("Power on success");
        }

        public static void PowerOff()
        {
            try
            {
                TokenSource.Cancel();
                Task.WaitAll(CurrentTasks.ToArray());
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
                TokenSource.Dispose();
                CurrentTasks = null;
            }
            Console.WriteLine("Power off success");
        }

        public static Task RunTask<T>(T sincronizador, CancellationToken cancellationToken, int delayLoop = 60000)
            where T : BaseSincronizador
            => Task.Run(() => sincronizador.SincronizarAsync(cancellationToken, delayLoop), cancellationToken);
    }    
}