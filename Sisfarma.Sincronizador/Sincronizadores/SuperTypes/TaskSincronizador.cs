using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Models;
using System;
using System.Linq;

namespace Sisfarma.Sincronizador.Sincronizadores.SuperTypes
{
    public abstract class TaskSincronizador : BaseSincronizador
    {
        public static void ClearConfiguracionPredefinida()
        {
            _configuracionPredefinida = null;
        }

        protected FarmaticService _farmatic;

        protected static ConfiguracionDictionary _configuracionPredefinida;

        public TaskSincronizador(FarmaticService farmatic, FisiotesService fisiotes)
            : base(fisiotes)
        {
            _farmatic = farmatic ?? throw new ArgumentNullException(nameof(farmatic));
        }

        protected ConfiguracionDictionary ConfiguracionPredefinida
        {
            get
            {
                if (_configuracionPredefinida == null)
                    _configuracionPredefinida = new ConfiguracionDictionary(
                        _fisiotes.Configuraciones.GetEstadosActuales()
                            .ToDictionary(k => k.campo, v => v.valor));

                return _configuracionPredefinida;
            }
        }
    }
}