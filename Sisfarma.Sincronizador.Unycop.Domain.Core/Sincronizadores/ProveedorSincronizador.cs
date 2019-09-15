using System.Threading.Tasks;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class ProveedorSincronizador : DC.ProveedorSincronizador
    {
        public ProveedorSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) :
            base(farmacia, fisiotes)
        { }

        public override void Process()
        {
            var proveedores = _farmacia.Proveedores.GetAll();
            foreach (var proveedor in proveedores)
            {
                Task.Delay(1);

                _cancellationToken.ThrowIfCancellationRequested();
                
                _sisfarma.Proveedores.Sincronizar(new Proveedor { idProveedor = proveedor.Id.ToString(), nombre = proveedor.Nombre });                
            }
        }
    }
}
