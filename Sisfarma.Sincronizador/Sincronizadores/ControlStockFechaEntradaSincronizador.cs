using Sisfarma.Sincronizador.Consejo;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Helpers;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class ControlStockFechaEntradaSincronizador : ControlSincronizador
    {
        private DateTime _ultimoFechaActualizacionStockSincronizado;

        public ControlStockFechaEntradaSincronizador(FarmaticService farmatic, FisiotesService fisiotes, ConsejoService consejo)
            : base(farmatic, fisiotes, consejo)
        { }

        public override void Process() => ProcessControlStockFechasEntrada();

        public override void PreSincronizacion()
        {
            var configuracion = _fisiotes.Configuraciones.GetByCampo(Configuracion.FIELD_STOCK_ENTRADA);

            _ultimoFechaActualizacionStockSincronizado = Calculator.CalculateFechaActualizacion(configuracion);
        }

        private void ProcessControlStockFechasEntrada()
        {
            var articulosWithIva = _farmatic.Articulos.GetByFechaUltimaEntradaGreaterOrEqual(_ultimoFechaActualizacionStockSincronizado);

            foreach (var articulo in articulosWithIva)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();
                var medicamentoGenerado = Generator.GenerarMedicamento(_farmatic, _consejo, articulo);
                _fisiotes.Medicamentos.Insert(medicamentoGenerado);

                _ultimoFechaActualizacionStockSincronizado = articulo.FechaUltimaEntrada ?? DateTime.Now;
            }
        }
    }
}