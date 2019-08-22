using Sisfarma.Sincronizador.Consejo;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Helpers;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class ControlStockFechaSalidaSincronizador : ControlSincronizador
    {
        private System.DateTime _ultimoFechaActualizacionStockSincronizado;

        public ControlStockFechaSalidaSincronizador(FarmaticService farmatic, FisiotesService fisiotes, ConsejoService consejo)
            : base(farmatic, fisiotes, consejo)
        { }

        public override void Process() => ProcessControlStockFechasSalida();

        public override void PreSincronizacion()
        {
            var configuracion = _fisiotes.Configuraciones.GetByCampo(Configuracion.FIELD_STOCK_SALIDA);

            _ultimoFechaActualizacionStockSincronizado = Calculator.CalculateFechaActualizacion(configuracion);
        }

        private void ProcessControlStockFechasSalida()
        {
            var articulosWithIva = _farmatic.Articulos.GetByFechaUltimaSalidaGreaterOrEqual(_ultimoFechaActualizacionStockSincronizado);

            foreach (var articulo in articulosWithIva)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();
                var medicamentoGenerado = Generator.GenerarMedicamento(_farmatic, _consejo, articulo);
                _fisiotes.Medicamentos.Insert(medicamentoGenerado);

                _ultimoFechaActualizacionStockSincronizado = articulo.FechaUltimaSalida ?? System.DateTime.Now;
            }
        }
    }
}