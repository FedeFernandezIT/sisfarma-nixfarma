using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class CategoriaSincronizador : TaskSincronizador
    {
        protected const string PADRE_DEFAULT = @"<SIN PADRE>";

        public CategoriaSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        { }

        public override void Process() => ProcessCategorias();

        private void ProcessCategorias()
        {
            var categorias = _farmacia.Categorias.GetAllByDescripcion();
            foreach (var categoria in categorias)
            {
                Task.Delay(5).Wait();
                _cancellationToken.ThrowIfCancellationRequested();

                var padre = _farmacia.Categorias.GetSubCategoriaById($"{categoria.Id}") 
                    ?? PADRE_DEFAULT;
                                                
                _sisfarma.Categorias.Sincronizar(new Categoria
                {
                    categoria = categoria.Nombre.Strip(),
                    padre = padre.Strip(),
                    prestashopPadreId = null                            
                });                
            }
        }
    }
}
