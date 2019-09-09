using Sisfarma.Sincronizador.Domain.Core.Services;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class CategoriaSincronizador : DC.CategoriaSincronizador
    {
        public CategoriaSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes)
            : base(farmacia, fisiotes)
        { }

        public override void Process()
        {
            var familias = _farmacia.Familias.GetByDescripcion();
            foreach (var familia in familias)
            {
                Task.Delay(5).Wait();
                _cancellationToken.ThrowIfCancellationRequested();

                _sisfarma.Categorias.Sincronizar(new Categoria
                {
                    categoria = familia.Nombre,
                    padre = familia.Padre,
                    prestashopPadreId = null
                });
            }
        }
    }
}