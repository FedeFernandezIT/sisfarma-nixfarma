using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

using FAR = Sisfarma.Sincronizador.Domain.Entities.Farmacia;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class PuntoPendienteSincronizador : DC.PuntoPendienteSincronizador
    {
        protected const string TIPO_CLASIFICACION_DEFAULT = "Familia";
        protected const string TIPO_CLASIFICACION_CATEGORIA = "Categoria";
        protected const string SISTEMA_UNYCOP = "unycop";

        private readonly ITicketRepository _ticketRepository;
        private readonly decimal _factorCentecimal = 0.01m;

        private string _clasificacion;
        private bool _debeCopiarClientes;
        private string _copiarClientes;
        private ICollection<int> _aniosProcesados;

        public PuntoPendienteSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes)
            : base(farmacia, fisiotes)
        {
            _ticketRepository = new TicketRepository();
            _aniosProcesados = new HashSet<int>();
        }

        public override void LoadConfiguration()
        {
            //base.LoadConfiguration();
            //_clasificacion = !string.IsNullOrWhiteSpace(ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION])
            //    ? ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION]
            //    : TIPO_CLASIFICACION_DEFAULT;

            //_copiarClientes = ConfiguracionPredefinida[Configuracion.FIELD_COPIAS_CLIENTES];
            //_debeCopiarClientes = _copiarClientes.ToLower().Equals("si") || string.IsNullOrWhiteSpace(_copiarClientes);
        }

        public override void PreSincronizacion()
        {
            //base.PreSincronizacion();
            //if (_ultimaVenta == 0 || _ultimaVenta == 1)
            //    _ultimaVenta = $"{_anioInicio}{_ultimaVenta}".ToIntegerOrDefault();
        }

        public override void Process()
        {
            //var anioProcesando = _aniosProcesados.Any() ? _aniosProcesados.Last() : $"{_ultimaVenta}".Substring(0, 4).ToIntegerOrDefault();

            //var ventaId = int.Parse($"{_ultimaVenta}".Substring(4));
            var anioProcesando = 2018;
            var ventaId = 1;
            var cargarPuntosSisfarma = true;
            var ventas = _farmacia.Ventas.GetAllByIdGreaterOrEqual(anioProcesando, ventaId);
            if (!ventas.Any())
            {
                //if (anioProcesando == DateTime.Now.Year)
                //    return;

                //_aniosProcesados.Add(anioProcesando + 1);
                //_ultimaVenta = $"{anioProcesando + 1 }{0}".ToIntegerOrDefault();
                return;
            }

            foreach (var venta in ventas)
            {
                Task.Delay(5).Wait();
                _cancellationToken.ThrowIfCancellationRequested();

                if (venta.ClienteId > 0)
                    venta.Cliente = _farmacia.Clientes.GetOneOrDefaultById(venta.ClienteId, cargarPuntosSisfarma);

                //var ticket = _ticketRepository.GetOneOrdefaultByVentaId(venta.Id, venta.FechaHora.Year);
                //if (ticket != null)
                //{
                //    venta.Ticket = new Ticket
                //    {
                //        Numero = ticket.Numero,
                //        Serie = ticket.Serie
                //    };
                //}

                //venta.VendedorNombre = _farmacia.Vendedores.GetOneOrDefaultById(venta.VendedorId)?.Nombre;
                //venta.Detalle = _farmacia.Ventas.GetDetalleDeVentaByVentaId($"{venta.FechaHora.Year}{venta.Id}".ToIntegerOrDefault());

                //if (venta.HasCliente() && _debeCopiarClientes)
                //{
                //    InsertOrUpdateCliente(venta.Cliente);
                //}

                //var puntosPendientes = GenerarPuntosPendientes(venta);
                //foreach (var puntoPendiente in puntosPendientes)
                //{
                //    _sisfarma.PuntosPendientes.Sincronizar(puntoPendiente);
                //}

                //_ultimaVenta = $"{venta.FechaHora.Year}{venta.Id}".ToIntegerOrDefault();
            }

            // <= 1 porque las ventas se recuperan con >= ventaID
            if (ventas.Count() <= 1)
            {
                _aniosProcesados.Add(anioProcesando + 1);
                _ultimaVenta = $"{anioProcesando + 1 }{0}".ToIntegerOrDefault();
            }

            var clientesConPuntos = ventas.Where(venta => venta.HasCliente())
                .Select(venta => venta.Cliente)
                .Where(cliente => cliente.Puntos > 0);
        }

        private IEnumerable<PuntosPendientes> GenerarPuntosPendientes(Venta venta)
        {
            if (!venta.HasCliente() && venta.Tipo != "1")
                return new PuntosPendientes[0];

            if (!venta.HasDetalle())
                return new PuntosPendientes[0];

            var puntosPendientes = new List<PuntosPendientes>();
            foreach (var item in venta.Detalle)
            {
                var familia = item.Farmaco.Familia?.Nombre ?? FAMILIA_DEFAULT;
                var puntoPendiente = new PuntosPendientes
                {
                    VentaId = $"{venta.FechaHora.Year}{venta.Id}".ToLongOrDefault(),
                    LineaNumero = item.Linea,
                    CodigoBarra = item.Farmaco.CodigoBarras ?? "847000" + item.Farmaco.Codigo.PadLeft(6, '0'),
                    CodigoNacional = item.Farmaco.Codigo,
                    Descripcion = item.Farmaco.Denominacion,

                    Familia = _clasificacion == TIPO_CLASIFICACION_CATEGORIA
                        ? item.Farmaco.Subcategoria?.Nombre ?? FAMILIA_DEFAULT
                        : familia,
                    SuperFamilia = _clasificacion == TIPO_CLASIFICACION_CATEGORIA
                        ? item.Farmaco.Categoria?.Nombre ?? FAMILIA_DEFAULT
                        : string.Empty,
                    SuperFamiliaAux = string.Empty,
                    FamiliaAux = _clasificacion == TIPO_CLASIFICACION_CATEGORIA ? familia : string.Empty,
                    CambioClasificacion = _clasificacion == TIPO_CLASIFICACION_CATEGORIA ? 1 : 0,

                    Cantidad = item.Cantidad,
                    Precio = item.Importe,
                    Pago = item.Equals(venta.Detalle.First()) ? venta.TotalBruto : 0,
                    TipoPago = venta.Tipo,
                    Fecha = venta.FechaHora.Date.ToDateInteger(),
                    DNI = venta.Cliente?.Id.ToString() ?? "0",
                    Cargado = _cargarPuntos.ToLower().Equals("si") ? "no" : "si",
                    Puesto = $"{venta.Puesto}",
                    Trabajador = venta.VendedorNombre,
                    LaboratorioCodigo = item.Farmaco.Laboratorio?.Codigo ?? string.Empty,
                    Laboratorio = item.Farmaco.Laboratorio?.Nombre ?? LABORATORIO_DEFAULT,
                    Proveedor = item.Farmaco.Proveedor?.Nombre ?? string.Empty,
                    Receta = item.Receta,
                    FechaVenta = venta.FechaHora,
                    PVP = item.PVP,
                    PUC = item.Farmaco?.PrecioCoste ?? 0,
                    Categoria = item.Farmaco.Categoria?.Nombre ?? string.Empty,
                    Subcategoria = item.Farmaco.Subcategoria?.Nombre ?? string.Empty,
                    VentaDescuento = item.Equals(venta.Detalle.First()) ? venta.TotalDescuento : 0,
                    LineaDescuento = item.Descuento,
                    TicketNumero = venta.Ticket?.Numero,
                    Serie = venta.Ticket?.Serie ?? string.Empty,
                    Sistema = SISTEMA_UNYCOP
                };

                puntosPendientes.Add(puntoPendiente);
            }

            return puntosPendientes;
        }

        private PuntosPendientes GenerarPuntoPendienteVentaSinDetalle(Venta venta)
        {
            return new PuntosPendientes
            {
                VentaId = $"{venta.FechaHora.Year}{venta.Id}".ToLongOrDefault(),
                LineaNumero = 1,
                CodigoBarra = string.Empty,
                CodigoNacional = "9999999",
                Descripcion = "Pago Deposito",

                Familia = FAMILIA_DEFAULT,
                SuperFamilia = _clasificacion == TIPO_CLASIFICACION_CATEGORIA
                    ? FAMILIA_DEFAULT
                    : string.Empty,
                SuperFamiliaAux = string.Empty,
                FamiliaAux = FAMILIA_DEFAULT,
                CambioClasificacion = _clasificacion == TIPO_CLASIFICACION_CATEGORIA ? 1 : 0,

                Cantidad = 0,
                Precio = venta.Importe,
                Pago = venta.TotalBruto,
                TipoPago = venta.Tipo,
                Fecha = venta.FechaHora.Date.ToDateInteger(),
                DNI = venta.Cliente?.Id.ToString() ?? "0",
                Cargado = _cargarPuntos.ToLower().Equals("si") ? "no" : "si",
                Puesto = $"{venta.Puesto}",
                Trabajador = venta.VendedorNombre,
                LaboratorioCodigo = string.Empty,
                Laboratorio = LABORATORIO_DEFAULT,
                Proveedor = string.Empty,
                Receta = string.Empty,
                FechaVenta = venta.FechaHora,
                PVP = 0,
                PUC = 0,
                Categoria = string.Empty,
                Subcategoria = string.Empty,
                VentaDescuento = venta.TotalDescuento,
                LineaDescuento = 0,
                TicketNumero = venta.Ticket?.Numero,
                Serie = venta.Ticket?.Serie ?? string.Empty,
                Sistema = SISTEMA_UNYCOP
            };
        }

        private void InsertOrUpdateCliente(FAR.Cliente cliente)
        {
            var debeCargarPuntos = _puntosDeSisfarma.ToLower().Equals("no") || string.IsNullOrWhiteSpace(_puntosDeSisfarma);

            if (_perteneceFarmazul)
            {
                var beBlue = _farmacia.Clientes.EsBeBlue($"{cliente.Id}");
                _sisfarma.Clientes.Sincronizar(cliente, beBlue, debeCargarPuntos);
            }
            else _sisfarma.Clientes.Sincronizar(cliente, debeCargarPuntos);
        }
    }
}