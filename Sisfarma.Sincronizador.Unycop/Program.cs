using Microsoft.Win32;
using Sisfarma.ClickOnce;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes;
using Sisfarma.Sincronizador.Unycop.Domain.Core.Config;
using Sisfarma.Sincronizador.Unycop.Domain.Core.Factories;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using Sisfarma.Sincronizador.Unycop.IoC.Factories;
using Sisfarma.Sincronizador.Unycop.Properties;
using System;
using System.Configuration;
using System.Deployment.Application;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Sisfarma.Sincronizador.Unycop
{
    internal static class Program
    {
        [STAThread]
        private static void Main()
        {
            if (!AppProcessHelper.SetSingleInstance())
            {
                Environment.Exit(-1);
            }

            ServicePointManager.DefaultConnectionLimit = 100;

            RegisterStartup(Globals.ProductName);
            var clickOnce = new ClickOnceHelper(Globals.PublisherName, Globals.ProductName);
            clickOnce.UpdateUninstallParameters();

            Initialize();

            //SisfarmaFactory.Create().Configuraciones.Update("versionSincronizador", $"{ApplicationDeployment.CurrentDeployment.CurrentVersion}");
            SisfarmaFactory.Create().Configuraciones.Update("versionSincronizador", "1.0");

            SincronizadorTaskManager.TaskSincronizadores
            .AddSincronizador(new Domain.Core.Sincronizadores.PuntoPendienteSincronizadorEmp1(
                farmacia: FarmaciaFactory.Create(),
                fisiotes: SisfarmaFactory.Create()),
                delay: SincronizadorTaskManager.DelayPuntosPendiente)
            .AddSincronizador(new Domain.Core.Sincronizadores.PuntoPendienteSincronizadorEmp2(
                farmacia: FarmaciaFactory.Create(),
                fisiotes: SisfarmaFactory.Create()),
                delay: SincronizadorTaskManager.DelayPuntosPendiente);
            //.AddSincronizador(new Domain.Core.Sincronizadores.ClienteSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //            .SetHorarioVaciemientos("1500", "2300"),
            //        delay: SincronizadorTaskManager.DelayClientes)
            //.AddSincronizador(new Domain.Core.Sincronizadores.HuecoSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayClientesHuecos)
            //.AddSincronizador(new Domain.Core.Sincronizadores.CategoriaSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayCategoria)
            //.AddSincronizador(new Domain.Core.Sincronizadores.ControlStockFechaEntradaSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayControlStockFechas)
            //.AddSincronizador(new Domain.Core.Sincronizadores.ControlStockFechaSalidaSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayControlStockFechas)
            //.AddSincronizador(new Domain.Core.Sincronizadores.ControlStockSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayControlStock)
            //.AddSincronizador(new Domain.Core.Sincronizadores.ControlSinStockSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayControlStock)
            //.AddSincronizador(new Domain.Core.Sincronizadores.ProductoCriticoSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayProductosCriticos)
            //.AddSincronizador(new Domain.Core.Sincronizadores.EncargoSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayEncargos)
            //.AddSincronizador(new Domain.Core.Sincronizadores.FamiliaSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayFamilia)
            //.AddSincronizador(new Domain.Core.Sincronizadores.ListaSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayListas)
            //.AddSincronizador(new Domain.Core.Sincronizadores.SinonimoSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //            .SetHorarioVaciamientos("1000", "1230", "1730", "1930"),
            //        delay: SincronizadorTaskManager.DelaySinomimos)
            //.AddSincronizador(new Domain.Core.Sincronizadores.PuntoPendienteActualizacionSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayPuntosPendiente)
            //.AddSincronizador(new Domain.Core.Sincronizadores.TicketPendienteActualizacionSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayVentaMensual)
            //.AddSincronizador(new Domain.Core.Sincronizadores.PagoPendienteActualizarSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayVentaMensual)
            //.AddSincronizador(new Domain.Core.Sincronizadores.PedidoSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayPedidos)
            //.AddSincronizador(new Domain.Core.Sincronizadores.ProveedorSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayProveedores)
            //.AddSincronizador(new Domain.Core.Sincronizadores.ProveedorHistorialSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create()),
            //        delay: SincronizadorTaskManager.DelayProveedoresHistorico)
            //.AddSincronizador(new Domain.Core.Sincronizadores.VentaMensualActualizacionSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create(),
            //        listaDeArticulo: FarmaciaContext.ListaDeArticulo),
            //        delay: SincronizadorTaskManager.DelayVentaMensual);

            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.PuntoPendienteSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //        .SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.ClienteSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //            .SetHorarioVaciemientos("1500", "2300")
            //.SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.HuecoSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //        .SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.ControlStockSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //        .SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.ControlSinStockSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //        .SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.ControlStockFechaEntradaSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //        .SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.ControlStockFechaSalidaSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //        .SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.PedidoSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //.SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.ProductoCriticoSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //        .SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.EncargoSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //        .SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.FamiliaSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //        .SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.ListaSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //        .SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.CategoriaSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //        .SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.SinonimoSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //            .SetHorarioVaciamientos("1000", "1230", "1730", "1930")
            //        .SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.ProveedorSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //        .SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            //Task.Factory.StartNew(() => new Domain.Core.Sincronizadores.ProveedorHistorialSincronizador(
            //        farmacia: FarmaciaFactory.Create(),
            //        fisiotes: SisfarmaFactory.Create())
            //        .SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 1));
            Task.Factory.StartNew(() => new PowerSwitchProgramado(SisfarmaFactory.Create()).SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 60000));
            Task.Factory.StartNew(() => new PowerSwitchManual(SisfarmaFactory.Create()).SincronizarAsync(Updater.GetCancellationToken(), delayLoop: 60000));
            Task.Factory.StartNew(() => new UpdateVersionSincronizador().SincronizarAsync(new CancellationToken(), delayLoop: 20000));

            var notifyIcon = new NotifyIcon();
            notifyIcon.ContextMenuStrip = GetSincronizadorMenuStrip();
            notifyIcon.Icon = Resources.sync;
            notifyIcon.Visible = true;
            Application.ApplicationExit += ApplicationExit;
            Application.ApplicationExit += (sender, @event) => notifyIcon.Visible = false;
            Application.Run(new SincronizadorApplication());
        }

        private static void ApplicationExit(object sender, EventArgs e)
        {
            // last change for cleanup code here!

            // only restart if user requested, not an unhandled app exception...
            AppProcessHelper.RestartIfRequired();
        }

        private static ContextMenuStrip GetSincronizadorMenuStrip()
        {
            var cms = new ContextMenuStrip();
            //cms.Items.Add($"Salir {ApplicationDeployment.CurrentDeployment.CurrentVersion}", null, (sender, @event) => Application.Exit());
            cms.Items.Add($"Salir", null, (sender, @event) => Application.Exit());
            return cms;
        }

        private static void Initialize()
        {
            try
            {
                var dir = ConfigurationManager.AppSettings["Directory.Setup"];

                var path = ConfigurationManager.AppSettings["File.Remote.Server"];
                var stream = new StreamReader(Path.Combine(dir, path));

                //var remoteServer = "https://sisfarma.es/api-cuadromandos";//stream.ReadLine();
                //var remoteToken = "f3d0b8171f8b6c1ed0566ca1570c86cc";//stream.ReadLine();
                var remoteServer = stream.ReadLine();
                var remoteToken = stream.ReadLine();
                SisfarmaFactory.Setup(remoteServer, remoteToken);

                var local = GetConnexionLocal(remoteServer, remoteToken);
                local.localServer = "192.168.0.30";

                FarmaciaContext.Setup(local.localServer, local.localUser, local.localPass, local.marketCodeList);
            }
            catch (IOException)
            {
                throw new IOException("Ha habido un error en la lectura de algún fichero de configuración. Compruebe que existen dichos ficheros de configuración.");
            }
        }

        internal static void RegisterStartup(string productName)
        {
            if (!ApplicationDeployment.IsNetworkDeployed)
                return;

            var location = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.Programs),
                @"Sisfarma.es", @"Sisfarma", "Sincronizador.Nixfarma.appref-ms");

            RegistryKey reg = Registry.CurrentUser.OpenSubKey("SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Run", true);
            reg.SetValue(productName, location);
        }

        private static LocalConfiguracion GetConnexionLocal(string server, string token)
        {
            //return new LocalConfiguracion
            //{
            //    pathFicheros = @"C:\Users\Federico\Documents\sisfarma\sincronizador\access\JM-ACCESS\TEST",
            //    pathFicheros = @"C:\Users\Federico\Documents\sisfarma\sincronizador\access\DATOS UNYCOP\DATOS UNYCOP",
            //    pathFicheros = @"C:\Users\Federico\Documents\sisfarma\sincronizador\access\JM",
            //    password = "BIGOTES",
            //    marketCodeList = -1
            //};

            try
            {
                var restClient = new RestClient.RestSharp.RestClient();

                var config = FisiotesConfig.TestConfig(server, token);

                var conn = restClient.BaseAddress(config.BaseAddress)
                    .UseAuthenticationBasic(config.Credentials.Token)
                    .Resource(config.Configuraciones.ConexionLocal)
                    .SendGet<LocalConfiguracion>();

                return conn;
            }
            catch (Exception)
            {
                return GetConnexionLocal(server, token);
            }
        }
    }
}