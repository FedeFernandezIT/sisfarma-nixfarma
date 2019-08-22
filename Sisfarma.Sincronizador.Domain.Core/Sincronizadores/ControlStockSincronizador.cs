using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class ControlStockSincronizador : ControlSincronizador
    {
        protected string _ultimoMedicamentoSincronizado;

        public ControlStockSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        { }
        
        public override void PreSincronizacion()
        {
            var valorConfiguracion = _sisfarma.Configuraciones.GetByCampo(Configuracion.FIELD_POR_DONDE_VOY_CON_STOCK);

            var codArticulo = !string.IsNullOrEmpty(valorConfiguracion)
                ? valorConfiguracion
                : "0";

            _ultimoMedicamentoSincronizado = codArticulo;
        }

        public override void Process() => throw new System.NotImplementedException();        
    }
}