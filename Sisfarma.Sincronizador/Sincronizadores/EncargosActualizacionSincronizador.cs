using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class EncargoActualizacionSincronizador : TaskSincronizador
    {
        public EncargoActualizacionSincronizador(FarmaticService farmatic, FisiotesService fisiotes) 
            : base(farmatic, fisiotes)
        {
        }

        public override void Process() => ProcessEncargoActualizacion();

        private void ProcessEncargoActualizacion()
        {
            var fechaInicial = DateTime.Now.Date.AddMonths(-2);

            var encargos = _farmatic.Encargos.GetAllGreaterOrEqualByFecha(fechaInicial);
            foreach (var encargo in encargos)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                if (!encargo.Estado.HasValue)
                    continue;

                if (encargo.Estado == 2 && encargo.FechaEntrega.HasValue)                
                    _fisiotes.Encargos.UpdateFechaDeEntrega(encargo.FechaEntrega.Value, encargo.IdContador);                                    
                else if (encargo.Estado == 3 && encargo.FechaRecepcion.HasValue)                
                    _fisiotes.Encargos.UpdateFechaDeRecepcion(encargo.FechaRecepcion.Value, encargo.IdContador);                        
            }            
        }
    }
}
