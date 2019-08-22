using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using CORE = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class HuecoSincronizador : CORE.HuecoSincronizador
    {
        public HuecoSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        { }

        public override void LoadConfiguration()
        {
            base.LoadConfiguration();
        }

        public override void PreSincronizacion()
        {
            base.PreSincronizacion();
        }

        public override void Process()
        {
            var remoteHuecos = _sisfarma.Huecos.GetByOrderAsc();

            foreach (var hueco in remoteHuecos)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                var cliente = _farmacia.Clientes.GetOneOrDefaultById(hueco.ToLongOrDefault());
                if (cliente != null)
                    InsertOrUpdateCliente(cliente);
            }
        }


        private void InsertOrUpdateCliente(Cliente cliente)
        {
            if (_perteneceFarmazul)
            {
                var beBlue = _farmacia.Clientes.EsBeBlue($"{cliente.Id}");
                _sisfarma.Clientes.SincronizarHueco(cliente, beBlue, _debeCargarPuntos);
            }
            else _sisfarma.Clientes.SincronizarHueco(cliente, _debeCargarPuntos);            
        }
    }
}
