using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia;
using CORE = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;
using DTO = Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia.DTO;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class ClienteSincronizador : CORE.ClienteSincronizador
    {
        public ClienteSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        { }

        public override void LoadConfiguration()
        {
            base.LoadConfiguration();
            _sisfarma.Clientes.ResetDniTracking();
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
                if (IsHoraVaciamientos())
                    Reset();

                var repository = _farmacia.Clientes as ClientesRepository;
                List<DTO.Cliente> localClientes = repository.GetGreatThanIdAsDTO(_ultimoClienteSincronizado);

                //var hueco = -1L;
                foreach (var cliente in localClientes)
                {
                    Task.Delay(5).Wait();
                    _cancellationToken.ThrowIfCancellationRequested();

                    //if (hueco == -1) hueco = cliente.Id;

                    InsertOrUpdateCliente(repository.GenerateCliente(cliente));

                    /*if (cliente.Id != hueco)
                    {
                        var huecos = new List<string>();
                        var batch = 0;
                        for (long i = hueco; i < cliente.Id; i++)
                        {
                            huecos.Add(i.ToString());
                            batch++;
                            if (batch == 1000)
                            {
                                _sisfarma.Huecos.Insert(huecos.ToArray());
                                huecos.Clear();
                                batch = 0;
                            }
                        }

                        if (huecos.Any())
                            _sisfarma.Huecos.Insert(huecos.ToArray());

                        hueco = cliente.Id;
                    }
                    hueco++;*/
                }
            }
        }


        private void InsertOrUpdateCliente(Cliente cliente)
        {            
            if (_perteneceFarmazul)
            {
                var beBlue = _farmacia.Clientes.EsBeBlue($"{cliente.Id}");
                _sisfarma.Clientes.Sincronizar(cliente, beBlue, _debeCargarPuntos);
            }
            else _sisfarma.Clientes.Sincronizar(cliente, _debeCargarPuntos);

            _ultimoClienteSincronizado = cliente.Id;
        }            
    }
}
