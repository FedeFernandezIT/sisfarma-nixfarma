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
    public class ProductoBorradoActualizacionSincronizador : TaskSincronizador
    {
        private string _ultimoMedicamentoSincronizado;

        public ProductoBorradoActualizacionSincronizador(FarmaticService farmatic, FisiotesService fisiotes)
            : base(farmatic, fisiotes)
        {
        }

        public override void Process() => ProcessUpdateProductosBorrados();

        public override void PreSincronizacion()
        {
            var valorConfiguracion = _fisiotes.Configuraciones.GetByCampo(Configuracion.FIELD_POR_DONDE_VOY_BORRAR);

            var codArticulo = !string.IsNullOrEmpty(valorConfiguracion)
                ? valorConfiguracion
                : "0";

            _ultimoMedicamentoSincronizado = codArticulo;
        }

        private void ProcessUpdateProductosBorrados()
        {
            var medicamentos = _fisiotes.Medicamentos
                .GetGreaterOrEqualCodigosNacionales(_ultimoMedicamentoSincronizado);

            if (!medicamentos.Any())
            {
                _fisiotes.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_BORRAR, "0");
                _ultimoMedicamentoSincronizado = "0";
                return;
            }

            if (medicamentos.Count() == 1)
            {
                var med = medicamentos.First();
                if (!_farmatic.Articulos.Exists(med.cod_nacional.PadLeft(6, '0')))
                    _fisiotes.Medicamentos.DeleteByCodigoNacional(med.cod_nacional);

                _fisiotes.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_BORRAR, "0");
                _ultimoMedicamentoSincronizado = "0";
            }
            else
            {
                foreach (var med in medicamentos)
                {
                    Task.Delay(5);

                    _cancellationToken.ThrowIfCancellationRequested();

                    if (!_farmatic.Articulos.Exists(med.cod_nacional.PadLeft(6, '0')))
                        _fisiotes.Medicamentos.DeleteByCodigoNacional(med.cod_nacional);

                    _ultimoMedicamentoSincronizado = med.cod_nacional;
                }
            }
        }
    }
}
