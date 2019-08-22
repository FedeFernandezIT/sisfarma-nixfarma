using System.Linq;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Consejo;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Helpers;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class ControlSinStockSincronizador : ControlSincronizador
    {
        private string _ultimoMedicamentoSincronizado;

        public ControlSinStockSincronizador(FarmaticService farmatic, FisiotesService fisiotes, ConsejoService consejo)
            : base(farmatic, fisiotes, consejo)
        { }

        public override void Process() => ProcessControlSinStockInicial();

        public override void PreSincronizacion()
        {
            var valorConfiguracion = _fisiotes.Configuraciones.GetByCampo(Configuracion.FIELD_POR_DONDE_VOY_SIN_STOCK);

            var codArticulo = !string.IsNullOrEmpty(valorConfiguracion)
                ? valorConfiguracion
                : "0";

            _ultimoMedicamentoSincronizado = codArticulo;
        }

        public void ProcessControlSinStockInicial()
        {            
            var articulos = _farmatic.Articulos.GetWithoutStockByIdGreaterOrEqual(_ultimoMedicamentoSincronizado);

            if (!articulos.Any())
            {
                _fisiotes.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_SIN_STOCK, "0");
                _ultimoMedicamentoSincronizado = "0";
                return;
            }

            foreach (var articulo in articulos)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                var medicamentoGenerado = Generator.GenerarMedicamento(_farmatic, _consejo, articulo);
                _fisiotes.Medicamentos.Insert(medicamentoGenerado);
                _ultimoMedicamentoSincronizado = articulo.IdArticu;
            }

            if (_farmatic.Articulos.GetControlArticuloSinStockFisrtOrDefault(articulos.Last().IdArticu) == null)
            {
                _fisiotes.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_SIN_STOCK, "0");
                _ultimoMedicamentoSincronizado = "0";
            }
        }
    }
}