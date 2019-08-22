using System;
using System.Linq;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class ProductoCriticoSincronizador : DC.ProductoCriticoSincronizador
    {
        protected const string TIPO_CLASIFICACION_DEFAULT = "Familia";
        protected const string TIPO_CLASIFICACION_CATEGORIA = "Categoria";
        protected const string SISTEMA_UNYCOP = "unycop";

        private string _clasificacion;

        public ProductoCriticoSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) : 
            base(farmacia, fisiotes)
        { }

        public override void LoadConfiguration()
        {
            base.LoadConfiguration();
            _clasificacion = !string.IsNullOrWhiteSpace(ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION])
                ? ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION]
                : TIPO_CLASIFICACION_DEFAULT;
        }

        public override void PreSincronizacion()
        {
            base.PreSincronizacion();
        }

        public override void Process()
        {  
            // _falta se carga en PreSincronizacion
            var pedidos = (_falta == null)
                ? _farmacia.Pedidos.GetAllByFechaGreaterOrEqual(new DateTime(DateTime.Now.Year - 2, 1, 1))
                : _farmacia.Pedidos.GetAllByIdGreaterOrEqual(_falta.idPedido);

            foreach (var pedido in pedidos)
            {
                Task.Delay(5).Wait();

                _cancellationToken.ThrowIfCancellationRequested();
                
                foreach (var linea in pedido.Detalle.Where(f => f.Farmaco.Stock == STOCK_CRITICO))
                {
                    Task.Delay(1).Wait();
                    
                    if (!_sisfarma.Faltas.ExistsLineaDePedido(linea.PedidoId, linea.Linea))                                            
                        _sisfarma.Faltas.Sincronizar(GenerarFaltante(linea));                    
                }

                if (_falta == null)
                    _falta = new Falta();

                _falta.idPedido = pedido.Id;
            }
        }

        private Falta GenerarFaltante(PedidoDetalle item)
        {
            var familia = item.Farmaco.Familia?.Nombre ?? FAMILIA_DEFAULT;

            var fechaPedido = item.Pedido.Fecha;
            var fechaActual = DateTime.Now;            

            return new Falta
            {
                idPedido = item.PedidoId,
                idLinea = item.Linea,
                cod_nacional = item.Farmaco.Codigo,
                descripcion = item.Farmaco.Denominacion,
                familia = _clasificacion == TIPO_CLASIFICACION_CATEGORIA
                        ? item.Farmaco.Subcategoria?.Nombre ?? FAMILIA_DEFAULT
                        : familia,
                superFamilia = _clasificacion == TIPO_CLASIFICACION_CATEGORIA
                        ? item.Farmaco.Categoria?.Nombre ?? FAMILIA_DEFAULT
                        : string.Empty,
                superFamiliaAux = string.Empty,
                familiaAux = _clasificacion == TIPO_CLASIFICACION_CATEGORIA ? familia : string.Empty,
                cambioClasificacion = _clasificacion == TIPO_CLASIFICACION_CATEGORIA,
                
                cantidadPedida = item.CantidadPedida,
                fechaFalta = fechaActual,
                cod_laboratorio = item.Farmaco.Laboratorio?.Codigo ?? string.Empty,
                laboratorio = item.Farmaco.Laboratorio?.Nombre ?? LABORATORIO_DEFAULT,
                proveedor = item.Farmaco.Proveedor?.Nombre ?? string.Empty,
                fechaPedido = fechaPedido,
                pvp = (float) item.Farmaco.Precio,
                puc = (float) item.Farmaco.PrecioCoste,
                sistema = SISTEMA_UNYCOP
            };
        }                
    }
}
