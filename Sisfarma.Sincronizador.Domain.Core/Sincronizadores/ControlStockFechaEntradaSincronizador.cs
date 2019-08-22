using Sisfarma.Sincronizador.Core.Helpers;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class ControlStockFechaEntradaSincronizador : ControlSincronizador
    {
        protected DateTime _ultimoFechaActualizacionStockSincronizado;

        public ControlStockFechaEntradaSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes)
            : base(farmacia, fisiotes)
        { }
        
        public override void PreSincronizacion()
        {
            var configuracion = _sisfarma.Configuraciones.GetByCampo(Configuracion.FIELD_STOCK_ENTRADA);

            _ultimoFechaActualizacionStockSincronizado = Calculator.CalculateFechaActualizacion(configuracion);
        }

        public override void Process() => throw new NotImplementedException();
    }    
}