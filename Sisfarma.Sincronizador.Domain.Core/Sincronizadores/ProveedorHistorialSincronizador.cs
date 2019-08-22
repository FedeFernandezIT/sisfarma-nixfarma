using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using System;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class ProveedorHistorialSincronizador : TaskSincronizador
    {
        protected readonly int _batchSize;

        protected DateTime _fechaMax;

        public ProveedorHistorialSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        {
            _batchSize = 1000;
        }

        public override void Process() => throw new NotImplementedException();

        public override void PreSincronizacion()
        {
            _fechaMax = _sisfarma.Proveedores.GetFechaMaximaDeHistorico() ?? new DateTime(DateTime.Now.Year - 2, 1, 1);
        }        
    }
}
