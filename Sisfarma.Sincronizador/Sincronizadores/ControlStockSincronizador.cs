using Sisfarma.Sincronizador.Consejo;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Helpers;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System.Linq;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class ControlStockSincronizador : ControlSincronizador
    {
        private string _ultimoMedicamentoSincronizado;

        public ControlStockSincronizador(FarmaticService farmatic, FisiotesService fisiotes, ConsejoService consejo)
            : base(farmatic, fisiotes, consejo)
        {
        }

        public override void Process() => ProcessControlStockInicial();

        public override void PreSincronizacion()
        {
            var valorConfiguracion = _fisiotes.Configuraciones.GetByCampo(Configuracion.FIELD_POR_DONDE_VOY_CON_STOCK);

            var codArticulo = !string.IsNullOrEmpty(valorConfiguracion)
                ? valorConfiguracion
                : "0";

            _ultimoMedicamentoSincronizado = codArticulo;
        }

        public void ProcessControlStockInicial()
        {            
            var articulos = _farmatic.Articulos.GetWithStockByIdGreaterOrEqual(_ultimoMedicamentoSincronizado);

            if (!articulos.Any())
            {
                _fisiotes.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_CON_STOCK, "0");
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

            if (_farmatic.Articulos.GetControlArticuloFisrtOrDefault(articulos.Last().IdArticu) == null)
            {
                _fisiotes.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_CON_STOCK, "0");
                _ultimoMedicamentoSincronizado = "0";
            }
        }
    }
}