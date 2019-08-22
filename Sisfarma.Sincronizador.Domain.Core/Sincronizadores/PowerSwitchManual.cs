using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class PowerSwitchManual : PowerSwitch
    {
        public PowerSwitchManual(ISisfarmaService sisfarma) 
            : base(sisfarma)
        {}

        public override void Process() => ProcessPowerSwitch();

        private void ProcessPowerSwitch()
        {
            var estadoActual = _sisfarma.Configuraciones
                .GetByCampo(FIELD_ENCENDIDO)
                    .ToLower()
                    .Trim();

            if (EstaEncendido && estadoActual == Programacion.Apagado)
                Apagar();
            else if (!EstaEncendido && estadoActual == Programacion.Encendido)
                Encender();
        }
    }
}