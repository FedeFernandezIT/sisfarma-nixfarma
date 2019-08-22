using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class PedidoSincronizador : TaskSincronizador
    {        
        protected const string LABORATORIO_DEFAULT = "<Sin Laboratorio>";
        protected const string FAMILIA_DEFAULT = "<Sin Clasificar>";
        
        protected int _anioInicio;
        protected Pedido _lastPedido;

        public PedidoSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        { }

        public override void LoadConfiguration()
        {
            _anioInicio = ConfiguracionPredefinida[Configuracion.FIELD_ANIO_INICIO]
                .ToIntegerOrDefault(@default: DateTime.Now.Year - 2);
        }

        public override void PreSincronizacion()
        {
            _lastPedido = _sisfarma.Pedidos.LastOrDefault();
        }

        public override void Process() => throw new NotImplementedException();
    }
}