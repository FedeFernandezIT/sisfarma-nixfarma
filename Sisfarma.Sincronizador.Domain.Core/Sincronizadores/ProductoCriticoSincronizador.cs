using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class ProductoCriticoSincronizador : TaskSincronizador
    {
        protected const string LABORATORIO_DEFAULT = "<Sin Laboratorio>";
        protected const string FAMILIA_DEFAULT = "<Sin Clasificar>";
        protected const int STOCK_CRITICO = 0;        
        
        protected Falta _falta;

        public ProductoCriticoSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        { }

        public override void PreSincronizacion()
        {
            _falta = _sisfarma.Faltas.LastOrDefault();
        }

        public override void Process() => throw new NotImplementedException();
    }
}
