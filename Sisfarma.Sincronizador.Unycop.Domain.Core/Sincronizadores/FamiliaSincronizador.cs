using System.Threading.Tasks;
using Sisfarma.Sincronizador.Domain.Core.Services;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class FamiliaSincronizador : DC.FamiliaSincronizador
    {
        public FamiliaSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        { }

        public override void Process()
        {
            var familias = _farmacia.Familias.GetAll();            
            foreach (var familia in familias)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();
                
                _sisfarma.Familias.Sincronizar(familia.Nombre);
            }

            var categorias = _farmacia.Categorias.GetAll();
            foreach (var categoria in categorias)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                _sisfarma.Familias.Sincronizar(categoria.Nombre, "Categoria");
            }

            var subcategorias = _farmacia.Subcategorias.GetAll();
            foreach (var subcategoria in subcategorias)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                _sisfarma.Familias.Sincronizar(subcategoria.Nombre, "Categoria");
            }
        }               
    }
}
