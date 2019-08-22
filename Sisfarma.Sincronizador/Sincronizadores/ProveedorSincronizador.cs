using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class ProveedorSincronizador : TaskSincronizador
    {
        public ProveedorSincronizador(FarmaticService farmatic, FisiotesService fisiotes) 
            : base(farmatic, fisiotes)
        {
        }

        public override void Process() => ProcessProveedor();

        private void ProcessProveedor()
        {
            var proveedores = _farmatic.Proveedores.GetAll();
            foreach (var pp in proveedores)
            {
                Task.Delay(1);

                _cancellationToken.ThrowIfCancellationRequested();

                var nombre = pp.FIS_NOMBRE.Strip();
                //var prov = _fisiotes.Proveedores.GetOneOrDefault(pp.IDPROVEEDOR, nombre);
                //if (prov == null)
                    _fisiotes.Proveedores.Insert(new Proveedor { idProveedor = pp.IDPROVEEDOR, nombre = nombre });
                //else _fisiotes.Proveedores.Update(new Proveedor { id = prov.id, idProveedor = pp.IDPROVEEDOR, nombre = nombre });
            }
        }
    }
}
