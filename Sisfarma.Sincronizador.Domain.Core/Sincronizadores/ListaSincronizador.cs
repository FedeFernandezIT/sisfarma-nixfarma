using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class ListaSincronizador : TaskSincronizador
    {
        protected const int BATCH_SIZE = 1000;
        protected long _codActual;

        public ListaSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes)
            : base(farmacia, fisiotes)
        { }

        public override void PreSincronizacion()
        {
            _codActual = _sisfarma.Listas.GetCodPorDondeVoyOrDefault()?.cod ?? -1;
        }

        public override void Process() => throw new NotImplementedException();
    }
}