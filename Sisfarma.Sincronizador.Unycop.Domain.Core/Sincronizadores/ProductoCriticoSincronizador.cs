using System;
using System.Linq;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Core.Extensions;
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
        protected const string SISTEMA_UNYCOP = "nixfarma";

        private string _clasificacion;
        private string _verCategorias;

        public ProductoCriticoSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) :
            base(farmacia, fisiotes)
        { }

        public override void LoadConfiguration()
        {
            base.LoadConfiguration();
            _clasificacion = !string.IsNullOrWhiteSpace(ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION])
                ? ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION]
                : TIPO_CLASIFICACION_DEFAULT;
            _verCategorias = ConfiguracionPredefinida[Configuracion.FIELD_VER_CATEGORIAS];
        }

        public override void PreSincronizacion()
        {
            base.PreSincronizacion();
        }

        public override void Process()
        {
            // _falta se carga en PreSincronizacion
            var fechaDefault = new DateTime(2013, 1, 1); // new DateTime(DateTime.Now.Year - 2, 1, 1);
            var pedidos = (_falta == null)
                ? _farmacia.Pedidos.GetAllByFechaGreaterOrEqual(fechaDefault)
                : _farmacia.Pedidos.GetAllByIdGreaterOrEqual(
                        long.Parse(_falta.idPedido.ToString().SubstringEnd(5)),
                        _falta.fechaPedido == DateTime.MinValue ? fechaDefault : _falta.fechaPedido);

            foreach (var pedido in pedidos)
            {
                var empresaSerial = pedido.Empresa == "EMP1" ? "00001" : "00002";
                pedido.Id = long.Parse($@"{pedido.Numero}{empresaSerial}");
                Task.Delay(5).Wait();

                _cancellationToken.ThrowIfCancellationRequested();

                var detalle = _farmacia.Pedidos.GetAllDetalleByPedido(pedido.Numero, pedido.Empresa, pedido.Fecha.Year);

                foreach (var linea in detalle.Where(f => f.Farmaco.Stock == STOCK_CRITICO))
                {
                    linea.Pedido = pedido;
                    linea.PedidoId = pedido.Id;
                    Task.Delay(1).Wait();

                    if (!_sisfarma.Faltas.ExistsLineaDePedido(linea.PedidoId, linea.Linea))
                        _sisfarma.Faltas.Sincronizar(GenerarFaltante(linea));
                }

                if (_falta == null)
                    _falta = new Falta();

                _falta.idPedido = pedido.Id;
                _falta.fechaPedido = pedido.Fecha;
            }
        }

        private Falta GenerarFaltante(PedidoDetalle item)
        {
            var fechaPedido = item.Pedido.Fecha;
            var fechaActual = DateTime.Now;

            var familia = !string.IsNullOrWhiteSpace(item.Farmaco.Familia?.Nombre) ? item.Farmaco.Familia.Nombre : FAMILIA_DEFAULT;
            var superFamilia = !string.IsNullOrWhiteSpace(item.Farmaco.SuperFamilia?.Nombre) ? item.Farmaco.SuperFamilia.Nombre : FAMILIA_DEFAULT;

            var categoria = item.Farmaco.Categoria?.Nombre;
            if (_verCategorias == "si" && !string.IsNullOrWhiteSpace(categoria) && categoria.ToLower() != "sin categoria" && categoria.ToLower() != "sin categoría")
            {
                if (string.IsNullOrEmpty(superFamilia) || superFamilia == FAMILIA_DEFAULT)
                    superFamilia = categoria;
                else superFamilia = $"{superFamilia} ~~~~~~~~ {categoria}";
            }

            return new Falta
            {
                idPedido = item.PedidoId,
                idLinea = item.Linea,
                cod_nacional = item.Farmaco.Codigo,
                descripcion = item.Farmaco.Denominacion,
                familia = familia,
                superFamilia = superFamilia,
                cambioClasificacion = _clasificacion == TIPO_CLASIFICACION_CATEGORIA,
                cantidadPedida = item.CantidadPedida,
                fechaFalta = fechaActual,
                cod_laboratorio = item.Farmaco.Laboratorio?.Codigo ?? string.Empty,
                laboratorio = item.Farmaco.Laboratorio?.Nombre ?? LABORATORIO_DEFAULT,
                proveedor = item.Farmaco.Proveedor?.Nombre ?? string.Empty,
                fechaPedido = fechaPedido,
                pvp = item.Farmaco.Precio,
                puc = item.Farmaco.PrecioCoste,
                sistema = SISTEMA_UNYCOP
            };
        }
    }
}