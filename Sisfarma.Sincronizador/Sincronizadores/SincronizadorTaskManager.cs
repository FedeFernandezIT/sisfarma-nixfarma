using Sisfarma.Sincronizador.Config;
using Sisfarma.Sincronizador.Consejo;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class SincronizadorTaskManager
    {
        public static ConcurrentBag<Task> CurrentTasks;
        public static CancellationTokenSource TokenSource;

        // milisegundos
        private static readonly int _delayCategoria = 3600000;
        private static readonly int _delayFamilia = 300000;
        private static readonly int _delayClientes = 30000;
        private static readonly int _delayClientesHuecos = 300000;
        private static readonly int _delayControlStock = 10000;
        private static readonly int _delayControlStockFechas = 60000;
        private static readonly int _delayEncargosActualizar = 60000;
        private static readonly int _delayListas = 300000;
        private static readonly int _delayEncargos = 60000;
        private static readonly int _delayProductosCriticos = 60000;
        private static readonly int _delayPedidos = 10000;
        private static readonly int _delayPuntosPendiente = 5000;
        private static readonly int _delayProductosBorrar = 60000;
        private static readonly int _delayVentaMensual = 300000;
        private static readonly int _delaySinomimos = 300000;
        private static readonly int _delayProveedores = 3600000;
        private static readonly int _delayProveedoresHistorico = 300000;
        private static readonly int _delayRecetaPendiente = 60000;

        private static ConcurrentBag<Task> CreateConcurrentTasks()
        {
            DisposeTasks();

            TokenSource = new CancellationTokenSource();
            var cancellationToken = TokenSource.Token;

            var listaDeCompra = LocalConfig.GetSingletonInstance().ListaDeCompras;

            CurrentTasks = new ConcurrentBag<Task>
            {
                RunTask(new ClienteSincronizador(FarmaticFactory.New(), FisiotesFactory.New()), cancellationToken, _delayClientes),

                RunTask(new HuecoSincronizador(FarmaticFactory.New(), FisiotesFactory.New()), cancellationToken, _delayClientesHuecos),

                RunTask(new PuntoPendienteSincronizador(FarmaticFactory.New(), FisiotesFactory.New(), new ConsejoService()), cancellationToken, _delayPuntosPendiente),

                RunTask(new SinonimoSincronizador(FarmaticFactory.New(), FisiotesFactory.New()), cancellationToken, _delaySinomimos),

                RunTask(new PedidoSincronizador(FarmaticFactory.New(), FisiotesFactory.New(), new ConsejoService()), cancellationToken, _delayPedidos),

                RunTask(new ProductoCriticoSincronizador(FarmaticFactory.New(), FisiotesFactory.New(), new ConsejoService()), cancellationToken, _delayProductosCriticos),

                RunTask(new FamiliaSincronizador(FarmaticFactory.New(), FisiotesFactory.New()), cancellationToken, _delayFamilia),

                RunTask(new RecetaPendienteActualizacionSincronizador(FarmaticFactory.New(), FisiotesFactory.New()), cancellationToken, _delayRecetaPendiente),

                //RunTask(new EntregaClienteActualizacionSincronizador(FarmaticFactory.New(), FisiotesFactory.New()), cancellationToken),

                RunTask(new ProductoBorradoActualizacionSincronizador(FarmaticFactory.New(), FisiotesFactory.New()), cancellationToken, _delayProductosBorrar),

                //RunTask(new PuntoPendienteActualizacionSincronizador(FarmaticFactory.New(), FisiotesFactory.New()), cancellationToken),

                RunTask(new ControlSinStockSincronizador(FarmaticFactory.New(), FisiotesFactory.New(), new ConsejoService()), cancellationToken, _delayControlStock),

                RunTask(new ControlStockSincronizador(FarmaticFactory.New(), FisiotesFactory.New(), new ConsejoService()), cancellationToken, _delayControlStock),

                RunTask(new ControlStockFechaEntradaSincronizador(FarmaticFactory.New(), FisiotesFactory.New(), new ConsejoService()), cancellationToken, _delayControlStockFechas),

                RunTask(new ControlStockFechaSalidaSincronizador(FarmaticFactory.New(), FisiotesFactory.New(), new ConsejoService()), cancellationToken, _delayControlStockFechas),

                RunTask(new EncargoSincronizador(FarmaticFactory.New(), FisiotesFactory.New(), new ConsejoService()), cancellationToken, _delayEncargos),

                RunTask(new CategoriaSincronizador(FarmaticFactory.New(), FisiotesFactory.New()), cancellationToken, _delayCategoria),

                RunTask(new ListaSincronizador(FarmaticFactory.New(), FisiotesFactory.New()), cancellationToken, _delayListas),

                //RunTask(new ListaTiendaSincronizador(FarmaticFactory.New(), FisiotesFactory.New(), new ConsejoService(), listaDeCompra), cancellationToken),

                //RunTask(new ListaFechaSincronizador(FarmaticFactory.New(), FisiotesFactory.New(), listaDeCompra), cancellationToken),

                RunTask(new VentaMensualActualizacionSincronizador(FarmaticFactory.New(), FisiotesFactory.New(), new ConsejoService(), listaDeCompra), cancellationToken, _delayVentaMensual),

                RunTask(new EncargoActualizacionSincronizador(FarmaticFactory.New(), FisiotesFactory.New()), cancellationToken, _delayEncargosActualizar),

                RunTask(new ProveedorHistorialSincronizador(FarmaticFactory.New(), FisiotesFactory.New()), cancellationToken, _delayProveedoresHistorico),

                RunTask(new ProveedorSincronizador(FarmaticFactory.New(), FisiotesFactory.New()), cancellationToken, _delayProveedores)
            };

            return CurrentTasks;
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