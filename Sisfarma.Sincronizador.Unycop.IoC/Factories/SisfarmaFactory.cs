using System;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Infrastructure.ExternalService.Fisiotes;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.ExternalServices;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.ExternalServices.Sisfarma;

using INF = Sisfarma.Sincronizador.Infrastructure.Fisiotes;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Factories
{
    public static class SisfarmaFactory
    {
        private static string _host;
        private static string _token;

        public static SisfarmaService Create()
        {
            var configuraciones = new INF.ConfiguracionesExternalService(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));
            var clientes = new ClientesExternalService(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));
            var huecos = new INF.HuecosExternalService(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));
            var puntosPendientes = new PuntosPendientesExternalService(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));
            var programacion = new INF.ProgramacionExternalService(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));
            var listas = new INF.ListasExternalService(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));
            var categorias = new INF.CategoriasExternalService(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));
            var encargos = new EncargosExternalService(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));
            var familias = new FamiliasExternalService(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));
            var medicamentos = new MedicamentosExternalServices(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));
            var sinonimos = new INF.SinonimosExternalService(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));
            var faltas = new FaltasExternalService(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));
            var pedidos = new PedidosExternalService(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));
            var proveedores = new INF.ProveedoresExternalService(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));
            var ventas = new VentasExternalService(new RestClient.WebClient.RestClient(), INF.FisiotesConfig.TestConfig(_host, _token));

            return new SisfarmaService(
                clientes: clientes,
                huecos: huecos,
                puntosPendientes: puntosPendientes,
                configuraciones: configuraciones,
                medicamentos: medicamentos,
                sinonimos: sinonimos,
                pedidos: pedidos,
                listas: listas,
                categorias: categorias,
                encargos: encargos,
                familias: familias,
                faltas: faltas,
                proveedores: proveedores,
                programacion: programacion,
                ventas: ventas);
        }

        public static void Setup(string host, string token)
        {
            if (string.IsNullOrWhiteSpace(host))
                throw new ArgumentException("message", nameof(host));

            if (string.IsNullOrWhiteSpace(token))
                throw new ArgumentException("message", nameof(token));

            _host = host;
            _token = token;
        }
    }
}