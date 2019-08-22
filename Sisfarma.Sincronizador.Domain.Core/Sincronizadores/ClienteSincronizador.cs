using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System;
using System.Linq;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class ClienteSincronizador : TaskSincronizador
    {
        protected string[] _horariosDeVaciamiento;
        
        protected string _puntosDeSisfarma;
        protected bool _perteneceFarmazul;
        protected bool _debeCargarPuntos;
        protected long _ultimoClienteSincronizado;
        protected string _copiarClientes;
        protected bool _debeCopiarClientes;

        public ClienteSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        {            
        }

        public override void LoadConfiguration()
        {
            _puntosDeSisfarma = ConfiguracionPredefinida[Configuracion.FIELD_PUNTOS_SISFARMA];            
            _perteneceFarmazul = bool.Parse(ConfiguracionPredefinida[Configuracion.FIELD_ES_FARMAZUL]);
            _debeCargarPuntos = _puntosDeSisfarma.ToLower().Equals("no") || string.IsNullOrWhiteSpace(_puntosDeSisfarma);
            _copiarClientes = ConfiguracionPredefinida[Configuracion.FIELD_COPIAS_CLIENTES];
            _debeCopiarClientes = _copiarClientes.ToLower().Equals("si") || string.IsNullOrWhiteSpace(_copiarClientes);
        }

        public override void PreSincronizacion()
        {
            _sisfarma.Clientes.ResetDniTracking();
            _ultimoClienteSincronizado = -1;
        }

        public ClienteSincronizador SetHorarioVaciemientos(params string[] hhmm)
        {
            _horariosDeVaciamiento = hhmm;
            return this;
        }

        public bool IsHoraVaciamientos()
        {
            if (_horariosDeVaciamiento == null)
                return false;

            return _horariosDeVaciamiento.Any(x => x.Equals(DateTime.Now.ToString("HHmm")));
        }

        public void Reset() => _ultimoClienteSincronizado = 0;

        public override void Process()
        {
            throw new NotImplementedException();
        }
    }
}
