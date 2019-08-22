using Sisfarma.Sincronizador.Fisiotes;
using static Sisfarma.Sincronizador.Fisiotes.Repositories.ConfiguracionesRepository;

namespace Sisfarma.Sincronizador.Sincronizadores.SuperTypes
{
    public abstract class PowerSwitch : BaseSincronizador
    {
        protected static bool EstaEncendido;

        protected const string FIELD_ENCENDIDO = FieldsConfiguracion.FIELD_ENCENDIDO;

        public PowerSwitch(FisiotesService fisiotes) 
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
            TaskSincronizador.ClearConfiguracionPredefinida();
            EstaEncendido = false;            
        }
    }
}
