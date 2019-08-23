using System;
using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.Services
{
    public interface ISisfarmaService
    {
        ICategoriasExternalService Categorias { get; set; }

        IClientesExternalService Clientes { get; }

        IConfiguracionesExternalService Configuraciones { get; }

        IEncargosExternalService Encargos { get; set; }

        IEntregasExternalService Entregas { get; }

        IFaltasExternalService Faltas { get; set; }

        IFamiliasExternalService Familias { get; set; }

        IHuecosExternalService Huecos { get; }

        IListasExternalService Listas { get; }

        IMedicamentosExternalService Medicamentos { get; }

        IPedidosExternalService Pedidos { get; }

        IProgramacionExternalService Programacion { get; set; }

        IProveedoresExternalService Proveedores { get; set; }

        IPuntosPendientesExternalService PuntosPendientes { get; }

        ISinonimosExternalService Sinonimos { get; }
    }

    public class SisfarmaService : ISisfarmaService
    {
        public IClientesExternalService Clientes { get; private set; }

        public IHuecosExternalService Huecos { get; private set; }

        public IPuntosPendientesExternalService PuntosPendientes { get; private set; }

        public IConfiguracionesExternalService Configuraciones { get; private set; }

        public IEntregasExternalService Entregas { get; private set; }

        public IMedicamentosExternalService Medicamentos { get; private set; }

        public ISinonimosExternalService Sinonimos { get; private set; }

        public IPedidosExternalService Pedidos { get; private set; }

        public IListasExternalService Listas { get; private set; }

        public ICategoriasExternalService Categorias { get; set; }

        public IEncargosExternalService Encargos { get; set; }

        public IFamiliasExternalService Familias { get; set; }

        public IFaltasExternalService Faltas { get; set; }

        public IProveedoresExternalService Proveedores { get; set; }

        public IProgramacionExternalService Programacion { get; set; }

        public SisfarmaService()
        {
        }

        public SisfarmaService(
            IClientesExternalService clientes,
            IHuecosExternalService huecos,
            IPuntosPendientesExternalService puntosPendientes,
            IConfiguracionesExternalService configuraciones,
            //IEntregasExternalService entregas,
            IMedicamentosExternalService medicamentos,
            ISinonimosExternalService sinonimos,
            IPedidosExternalService pedidos,
            IListasExternalService listas,
            ICategoriasExternalService categorias,
            IEncargosExternalService encargos,
            IFamiliasExternalService familias,
            IFaltasExternalService faltas,
            IProveedoresExternalService proveedores,
            IProgramacionExternalService programacion)
        {
            Clientes = clientes ?? throw new ArgumentNullException(nameof(clientes));
            Huecos = huecos ?? throw new ArgumentNullException(nameof(huecos));
            PuntosPendientes = puntosPendientes ?? throw new ArgumentNullException(nameof(puntosPendientes));
            Configuraciones = configuraciones ?? throw new ArgumentNullException(nameof(configuraciones));
            //Entregas = entregas ?? throw new ArgumentNullException(nameof(entregas));
            Medicamentos = medicamentos ?? throw new ArgumentNullException(nameof(medicamentos));
            Sinonimos = sinonimos ?? throw new ArgumentNullException(nameof(sinonimos));
            Pedidos = pedidos ?? throw new ArgumentNullException(nameof(pedidos));
            Listas = listas ?? throw new ArgumentNullException(nameof(listas));
            Categorias = categorias ?? throw new ArgumentNullException(nameof(categorias));
            Encargos = encargos ?? throw new ArgumentNullException(nameof(encargos));
            Familias = familias ?? throw new ArgumentNullException(nameof(familias));
            Faltas = faltas ?? throw new ArgumentNullException(nameof(faltas));
            Proveedores = proveedores ?? throw new ArgumentNullException(nameof(proveedores));
            Programacion = programacion ?? throw new ArgumentNullException(nameof(programacion));
        }

        public SisfarmaService(string host, string token)
        {
            //Clientes = new ClientesExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            //Huecos = new HuecosExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            //PuntosPendientes = new PuntosPendientesExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            //Configuraciones = new ConfiguracionesExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            //Entregas = new EntregasExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            //Medicamentos = new MedicamentosExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            //Sinonimos = new SinonimosExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            //Pedidos = new PedidosExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            //Faltas = new FaltasExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            //Familias = new FamiliasExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            //Encargos = new EncargosExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            //Categorias = new CategoriasExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            //Listas = new ListasExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            //Proveedores = new ProveedoresExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            //Programacion = new ProgramacionExternalService(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
        }

        public SisfarmaService(RemoteConfig config)
            : this(config.Server, config.Token)
        { }
    }
}