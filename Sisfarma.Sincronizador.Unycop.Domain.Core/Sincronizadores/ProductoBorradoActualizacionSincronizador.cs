using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System.Linq;
using System.Threading.Tasks;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class ProductoBorradoActualizacionSincronizador : DC.ProductoBorradoActualizacionSincronizador
    {
        public ProductoBorradoActualizacionSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        { }

        public override void Process()
        {
            var medicamentos = _sisfarma.Medicamentos
                .GetGreaterOrEqualCodigosNacionales(_ultimoMedicamentoSincronizado);

            if (!medicamentos.Any())
            {
                _sisfarma.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_BORRAR, "0");
                _ultimoMedicamentoSincronizado = "0";
                return;
            }

            if (medicamentos.Count() == 1)
            {
                var med = medicamentos.First();
                if (!_farmacia.Farmacos.Exists(med.cod_nacional))
                    _sisfarma.Medicamentos.DeleteByCodigoNacional(med.cod_nacional);

                _sisfarma.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_BORRAR, "0");
                _ultimoMedicamentoSincronizado = "0";
            }
            else
            {
                foreach (var med in medicamentos)
                {
                    Task.Delay(5).Wait();

                    _cancellationToken.ThrowIfCancellationRequested();

                    if (!_farmacia.Farmacos.Exists(med.cod_nacional))
                        _sisfarma.Medicamentos.DeleteByCodigoNacional(med.cod_nacional);

                    _ultimoMedicamentoSincronizado = med.cod_nacional;
                }
            }
        }
    }
}
