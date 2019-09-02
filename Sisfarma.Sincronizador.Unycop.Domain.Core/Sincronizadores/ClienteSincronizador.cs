using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia;
using CORE = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;
using DTO = Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia.DTO;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class ClienteSincronizador : CORE.ClienteSincronizador
    {
        private string _filtrosResidencia;
        private string _cargarPuntos;

        public ClienteSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes)
            : base(farmacia, fisiotes)
        { }

        public override void LoadConfiguration()
        {
            base.LoadConfiguration();
            _sisfarma.Clientes.ResetDniTracking();
            _filtrosResidencia = ConfiguracionPredefinida[Configuracion.FIELD_FILTROS_RESIDENCIA];
            _cargarPuntos = ConfiguracionPredefinida[Configuracion.FIELD_CARGAR_PUNTOS] ?? "no";
            Reset();
        }

        public override void PreSincronizacion()
        {
            base.PreSincronizacion();
        }

        public override void Process()
        {
            if (_debeCopiarClientes)
            {
                var cargarPuntosSisfarma = _cargarPuntos == "si";

                if (IsHoraVaciamientos())
                    Reset();

                var repository = _farmacia.Clientes as ClientesRepository;
                var localClientes = repository.GetGreatThanIdAsDTO(_ultimoClienteSincronizado, cargarPuntosSisfarma);

                foreach (var cliente in localClientes)
                {
                    Task.Delay(5).Wait();
                    _cancellationToken.ThrowIfCancellationRequested();

                    InsertOrUpdateCliente(cliente);
                }
            }
        }

        private void InsertOrUpdateCliente(Sincronizador.Domain.Entities.Farmacia.Cliente cliente)
        {
            cliente.Tipo = _farmacia.Clientes.EsResidencia($"{cliente.CodigoCliente}", $"{cliente.CodigoDes}", _filtrosResidencia);

            if (_perteneceFarmazul)
            {
                var beBlue = _farmacia.Clientes.EsBeBlue($"{cliente.CodigoCliente}", $"{cliente.CodigoDes}");
                _sisfarma.Clientes.Sincronizar(cliente, beBlue, _debeCargarPuntos);
            }
            else _sisfarma.Clientes.Sincronizar(cliente, _debeCargarPuntos);

            _ultimoClienteSincronizado = cliente.Id;
        }
    }
}