using System.Threading.Tasks;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class FamiliaSincronizador : DC.FamiliaSincronizador
    {
        private string _verCategorias;

        public FamiliaSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes)
            : base(farmacia, fisiotes)
        { }

        public override void LoadConfiguration()
        {
            base.LoadConfiguration();
            _verCategorias = ConfiguracionPredefinida[Configuracion.FIELD_VER_CATEGORIAS];
        }

        public override void Process()
        {
            var tipo = _verCategorias == "si" ? "Familia" : null;

            var familias = _farmacia.Familias.GetAll();
            foreach (var familia in familias)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                _sisfarma.Familias.Sincronizar(familia.Nombre, tipo);
            }

            var subfamilias = _farmacia.Familias.GetAllSubFamilias();
            foreach (var familia in subfamilias)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                _sisfarma.Familias.Sincronizar(familia.Nombre, tipo);
            }

            if (_verCategorias == "si")
            {
                var categorias = _farmacia.Categorias.GetAll();
                foreach (var categoria in categorias)
                {
                    Task.Delay(5);

                    _cancellationToken.ThrowIfCancellationRequested();

                    _sisfarma.Familias.Sincronizar(categoria.Nombre, "Categoria");
                }
            }
        }
    }
}