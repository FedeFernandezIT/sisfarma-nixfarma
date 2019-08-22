using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class PowerSwitchManual : PowerSwitch
    {
        public PowerSwitchManual(FisiotesService fisiotes)
            : base(fisiotes)
        { }

        public override void Process() => ProcessPowerSwitch();

        private void ProcessPowerSwitch()
        {
            var estadoActual = _fisiotes.Configuraciones
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