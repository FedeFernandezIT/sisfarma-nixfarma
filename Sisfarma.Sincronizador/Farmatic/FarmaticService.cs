using Sisfarma.Sincronizador.Config;
using Sisfarma.Sincronizador.Farmatic.Repositories;
using System;

namespace Sisfarma.Sincronizador.Farmatic
{
    public class FarmaticService
    {
        private LocalConfig _config;

        public ClientesRepository Clientes { get; private set; }

        public DestinatariosRepository Destinatarios { get; private set; }

        public VendedoresRepository Vendedores { get; private set; }

        public VentasRepository Ventas { get; private set; }

        public ArticulosRepository Articulos { get; private set; }

        public ProveedoresRepository Proveedores { get; private set; }

        public SinonimosRepository Sinonimos { get; private set; }

        public FamiliasRepository Familias { get; private set; }

        public LaboratoriosRepository Laboratorios { get; private set; }

        public RecepcionesRepository Recepciones { get; private set; }

        public ListasArticulosRepository ListasArticulos { get; private set; }

        public EncargosRepository Encargos { get; private set; }

        public PedidosRepository Pedidos { get; private set; }

        public FarmaticService(LocalConfig config)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));

            Clientes = new ClientesRepository(_config);
            Destinatarios = new DestinatariosRepository(_config);
            Vendedores = new VendedoresRepository(_config);
            Ventas = new VentasRepository(_config);
            Articulos = new ArticulosRepository(_config);
            Proveedores = new ProveedoresRepository(_config);
            Sinonimos = new SinonimosRepository(_config);
            Familias = new FamiliasRepository(_config);
            Laboratorios = new LaboratoriosRepository(_config);
            Recepciones = new RecepcionesRepository(_config);
            ListasArticulos = new ListasArticulosRepository(_config);
            Encargos = new EncargosRepository(_config);
            Pedidos = new PedidosRepository(_config);
        }
    }
}