using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes
{
    public abstract class PowerSwitch : BaseSincronizador
    {
        protected static bool EstaEncendido;

        protected const string FIELD_ENCENDIDO = Configuracion.FIELD_ENCENDIDO;

        public PowerSwitch(ISisfarmaService fisiotes) 
            : base(fisiotes)
        { }        

        protected virtual void Encender()
        {
            SincronizadorTaskManager.PowerOn();
            EstaEncendido = true;            
        }

        protected virtual void Apagar()
        {
            SincronizadorTaskManager.PowerOff();
            EstaEncendido = false;            
        }
    }
}
