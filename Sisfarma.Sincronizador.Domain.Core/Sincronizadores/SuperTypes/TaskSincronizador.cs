using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Models;
using System;
using System.Linq;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes
{
    public abstract class TaskSincronizador : BaseSincronizador
    {
        protected IFarmaciaService _farmacia;

        protected static ConfiguracionDictionary _configuracionPredefinida;

        public TaskSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes)
            : base(fisiotes)
        {
            _farmacia = farmacia ?? throw new ArgumentNullException(nameof(farmacia));
        }

        protected ConfiguracionDictionary ConfiguracionPredefinida
        {
            get
            {
                if (_configuracionPredefinida == null)
                    _configuracionPredefinida = new ConfiguracionDictionary(
                        _sisfarma.Configuraciones.GetEstadosActuales()
                            .ToDictionary(k => k.campo, v => v.valor));

                return _configuracionPredefinida;
            }
        }
    }
}