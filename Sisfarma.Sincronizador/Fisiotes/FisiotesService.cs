using Sisfarma.Sincronizador.Config;
using Sisfarma.Sincronizador.Fisiotes.Repositories;

namespace Sisfarma.Sincronizador.Fisiotes
{
    public class FisiotesService
    {
        public ClientesRepository Clientes { get; private set; }

        public HuecosRepository Huecos { get; private set; }

        public PuntosPendientesRepository PuntosPendientes { get; private set; }

        public ConfiguracionesRepository Configuraciones { get; private set; }

        public EntregasRepository Entregas { get; private set; }

        public MedicamentosRepository Medicamentos { get; private set; }

        public SinonimosRepository Sinonimos { get; private set; }

        public PedidosRepository Pedidos { get; private set; }

        public ListasRepository Listas { get; private set; }

        public CategoriasRepository Categorias { get; set; }

        public EncargosRepository Encargos { get; set; }

        public FamiliasRepository Familias { get; set; }

        public FaltasRepository Faltas { get; set; }

        public ProveedoresRepository Proveedores { get; set; }

        public ProgramacionRepository Programacion { get; set; }

        public FisiotesService(string host, string token)
        {
            Clientes = new ClientesRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            Huecos = new HuecosRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            PuntosPendientes = new PuntosPendientesRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            Configuraciones = new ConfiguracionesRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            Entregas = new EntregasRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            Medicamentos = new MedicamentosRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            Sinonimos = new SinonimosRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            Pedidos = new PedidosRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            Faltas = new FaltasRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            Familias = new FamiliasRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            Encargos = new EncargosRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            Categorias = new CategoriasRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            Listas = new ListasRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            Proveedores = new ProveedoresRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
            Programacion = new ProgramacionRepository(new RestClient.RestSharp.RestClient(), FisiotesConfig.TestConfig(host, token));
        }

        public FisiotesService(RemoteConfig config)
            : this(config.Server, config.Token)
        { }
    }
}