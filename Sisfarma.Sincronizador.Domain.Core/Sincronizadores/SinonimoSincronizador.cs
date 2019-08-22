using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class SinonimoSincronizador : TaskSincronizador
    {
        protected string[] _horariosDeVaciamiento;
        protected readonly int _batchSize;

        protected bool _isEmpty;

        public SinonimoSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        {            
            _batchSize = 1000;
        }

        public SinonimoSincronizador SetHorarioVaciamientos(params string[] hhmm)
        {
            _horariosDeVaciamiento = hhmm;
            return this;
        }

        public override void Process() => throw new NotImplementedException();

        public override void PreSincronizacion()
        {
            _isEmpty = _sisfarma.Sinonimos.IsEmpty();
        }        
    }
}