using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System;
using System.Linq;
using System.Threading.Tasks;
using static Sisfarma.Sincronizador.Fisiotes.Repositories.ConfiguracionesRepository;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class RecetaPendienteActualizacionSincronizador : TaskSincronizador
    {
        private int _anioInicio;

        public RecetaPendienteActualizacionSincronizador(FarmaciaService farmatic, FisiotesService fisiotes) 
            : base(farmatic, fisiotes)
        {
        }

        public override void LoadConfiguration()
        {
            _anioInicio = ConfiguracionPredefinida[Configuracion.FIELD_ANIO_INICIO]
                .ToIntegerOrDefault(@default: DateTime.Now.Year - 2);
        }

        public override void Process() => ProcessUpdateRecetasPendientes();

        private void ProcessUpdateRecetasPendientes()
        {
            var puntos = _fisiotes.PuntosPendientes.GetOfRecetasPendientes(_anioInicio);
                
            foreach (var punto in puntos)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                var lineaVenta = _farmatic.Ventas.GetLineaVentaOrDefaultByKey(punto.idventa, punto.idnlinea);
                    
                if (lineaVenta != null)
                {
                    if (string.IsNullOrEmpty(punto.recetaPendiente) || lineaVenta.RecetaPendiente!= "D")
                    {
                        _fisiotes.PuntosPendientes.Update(punto.idventa, punto.idnlinea, lineaVenta.RecetaPendiente);
                    }                    
                }                    
                else
                {
                    if (string.IsNullOrEmpty(punto.recetaPendiente))
                    {
                        _fisiotes.PuntosPendientes.Update(punto.idventa, punto.idnlinea);
                    }
                } 

                    
            }
        }
    }
}
