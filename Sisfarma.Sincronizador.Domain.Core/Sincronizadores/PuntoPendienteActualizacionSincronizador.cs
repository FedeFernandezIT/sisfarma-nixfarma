using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using System;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class PuntoPendienteActualizacionSincronizador : TaskSincronizador
    {
        public PuntoPendienteActualizacionSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        { }

        public override void Process() => throw new NotImplementedException();        
    }
}
