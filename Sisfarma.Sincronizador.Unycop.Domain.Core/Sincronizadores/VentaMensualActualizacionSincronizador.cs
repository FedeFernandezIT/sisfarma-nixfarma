using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;
using FAR = Sisfarma.Sincronizador.Domain.Entities.Farmacia;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class VentaMensualActualizacionSincronizadorEmp1 : DC.VentaMensualActualizacionSincronizador
    {
        protected const string TIPO_CLASIFICACION_DEFAULT = "Familia";
        protected const string TIPO_CLASIFICACION_CATEGORIA = "Categoria";
        protected const string SISTEMA_NIXFARMA = "nixfarma";

        private string _clasificacion;
        private bool _debeCopiarClientes;
        private string _copiarClientes;
        private int _anioInicio;
        private string _verCategorias;
        private string _filtrosResidencia;
        private string _codigoEmpresa = "00001";

        public VentaMensualActualizacionSincronizadorEmp1(IFarmaciaService farmacia, ISisfarmaService fisiotes, int listaDeArticulo)
            : base(farmacia, fisiotes, listaDeArticulo)
        { }

        public override void LoadConfiguration()
        {
            base.LoadConfiguration();
            _clasificacion = !string.IsNullOrWhiteSpace(ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION])
                ? ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION]
                : TIPO_CLASIFICACION_DEFAULT;

            _copiarClientes = ConfiguracionPredefinida[Configuracion.FIELD_COPIAS_CLIENTES];
            _debeCopiarClientes = _copiarClientes.ToLower().Equals("si") || string.IsNullOrWhiteSpace(_copiarClientes);
            _anioInicio = ConfiguracionPredefinida[Configuracion.FIELD_ANIO_INICIO]
               .ToIntegerOrDefault(@default: DateTime.Now.Year - 2);
            _verCategorias = ConfiguracionPredefinida[Configuracion.FIELD_VER_CATEGORIAS];
            _puntosDeSisfarma = ConfiguracionPredefinida[Configuracion.FIELD_PUNTOS_SISFARMA];
            _filtrosResidencia = ConfiguracionPredefinida[Configuracion.FIELD_FILTROS_RESIDENCIA];
        }

        public override void Process()
        {
            var cargarPuntosSisfarma = _cargarPuntos == "si";
            var fechaActual = DateTime.Now.Date;
            if (!FechaConfiguracionIsValid(fechaActual, Configuracion.FIELD_POR_DONDE_VOY_VENTA_MES_EMP1))
                return;

            var fechaInicial = CalcularFechaInicialDelProceso(fechaActual);
            if (!_sisfarma.PuntosPendientes.ExistsGreatThanOrEqual(fechaInicial, _codigoEmpresa))
                return;

            var ventaIdConfiguracion = _sisfarma.Configuraciones
                .GetByCampo(Configuracion.FIELD_POR_DONDE_VOY_VENTA_MES_ID_EMP1)
                    .ToIntegerOrDefault();

            var ventas = _farmacia.Ventas.GetAllByIdGreaterOrEqual(ventaIdConfiguracion, fechaInicial, "EMP1");
            var batchPuntosPendientes = new List<PuntosPendientes>();
            foreach (var venta in ventas)
            {
                Task.Delay(5).Wait();
                _cancellationToken.ThrowIfCancellationRequested();

                if (venta.ClienteId > 0)
                    venta.Cliente = _farmacia.Clientes.GetOneOrDefaultById(venta.ClienteId, cargarPuntosSisfarma);

                //venta.VendedorNombre = _farmacia.Vendedores.GetOneOrDefaultById(venta.VendedorId)?.Nombre;
                venta.Detalle = _farmacia.Ventas.GetDetalleDeVentaByVentaId(venta.Operacion, "EMP1");

                if (venta.HasCliente() && _debeCopiarClientes)
                    InsertOrUpdateCliente(venta.Cliente);

                var puntosPendientes = GenerarPuntosPendientes(venta);
                batchPuntosPendientes.AddRange(puntosPendientes);                
            }

            if (batchPuntosPendientes.Any()) _sisfarma.PuntosPendientes.Sincronizar(batchPuntosPendientes, calcularPuntos: true);


            _sisfarma.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_VENTA_MES_ID_EMP1, "0");
            _sisfarma.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_VENTA_MES_EMP1, fechaActual.ToString("yyyy-MM-dd"));
        }

        private DateTime CalcularFechaInicialDelProceso(DateTime fechaActual)
        {
            var mesConfiguracion = ConfiguracionPredefinida[Configuracion.FIELD_REVISAR_VENTA_MES_DESDE].ToIntegerOrDefault();
            var mesRevision = (mesConfiguracion > 0) ? -mesConfiguracion : -1;
            return fechaActual.AddMonths(mesRevision);
        }

        private bool FechaConfiguracionIsValid(DateTime fechaActual, string fechaVentaConfiguracion)
        {
            var fechaConfiguracion = _sisfarma.Configuraciones.GetByCampo(fechaVentaConfiguracion).ToDateTimeOrDefault("yyyy-MM-dd");
            return fechaActual.Date != fechaConfiguracion.Date;
        }

        private IEnumerable<PuntosPendientes> GenerarPuntosPendientes(FAR.Venta venta)
        {
            if (!venta.HasDetalle())
                return new PuntosPendientes[0];

            var puntosPendientes = new List<PuntosPendientes>();
            foreach (var item in venta.Detalle.Where(d => d.HasFarmaco()))
            {
                var familia = !string.IsNullOrWhiteSpace(item.Farmaco.Familia?.Nombre) ? item.Farmaco.Familia.Nombre : FAMILIA_DEFAULT;
                var superFamilia = !string.IsNullOrWhiteSpace(item.Farmaco.SuperFamilia?.Nombre) ? item.Farmaco.SuperFamilia.Nombre : FAMILIA_DEFAULT;

                var categoria = item.Farmaco.Categoria?.Nombre;
                if (_verCategorias == "si" && !string.IsNullOrWhiteSpace(categoria) && categoria.ToLower() != "sin categoria" && categoria.ToLower() != "sin categoría")
                {
                    if (string.IsNullOrEmpty(superFamilia) || superFamilia == FAMILIA_DEFAULT)
                        superFamilia = categoria;
                    else superFamilia = $"{superFamilia} ~~~~~~~~ {categoria}";
                }

                var puntoPendiente = new PuntosPendientes
                {
                    VentaId = $"{venta.Operacion}{_codigoEmpresa}".ToLongOrDefault(),
                    LineaNumero = item.Linea,
                    CodigoBarra = item.Farmaco.CodigoBarras ?? "847000" + item.Farmaco.Codigo.PadLeft(6, '0'),
                    CodigoNacional = item.Farmaco.Codigo,
                    Descripcion = item.Farmaco.Denominacion,

                    Familia = familia,
                    SuperFamilia = superFamilia,
                    SuperFamiliaAux = string.Empty,
                    FamiliaAux = string.Empty,
                    CambioClasificacion = _clasificacion == TIPO_CLASIFICACION_CATEGORIA ? 1 : 0,

                    Cantidad = item.Cantidad,
                    Precio = item.Precio,
                    Pago = item.Equals(venta.Detalle.First()) ? venta.TotalBruto : 0,
                    TipoPago = venta.TipoOperacion,
                    Fecha = venta.FechaHora.Date.ToDateInteger(),
                    DNI = venta.Cliente?.Id.ToString() ?? "0",
                    Cargado = _cargarPuntos.ToLower().Equals("si") ? "no" : "si",
                    Puesto = $"{venta.Puesto}",
                    Trabajador = !string.IsNullOrWhiteSpace(venta.VendedorCodigo) ? venta.VendedorCodigo.Trim() : string.Empty,
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
                    Sistema = SISTEMA_NIXFARMA,
                    Ubicacion = item.Farmaco?.Ubicacion
                };

                puntosPendientes.Add(puntoPendiente);
            }

            return puntosPendientes;
        }

        private void InsertOrUpdateCliente(FAR.Cliente cliente)
        {
            var debeCargarPuntos = _puntosDeSisfarma.ToLower().Equals("no") || string.IsNullOrWhiteSpace(_puntosDeSisfarma);
            cliente.DebeCargarPuntos = debeCargarPuntos;

            cliente.Tipo = _farmacia.Clientes.EsResidencia($"{cliente.CodigoCliente}", $"{cliente.CodigoDes}", _filtrosResidencia);

            if (_perteneceFarmazul)
            {
                var beBlue = _farmacia.Clientes.EsBeBlue($"{cliente.CodigoCliente}", $"{cliente.CodigoDes}");
                cliente.BeBlue = beBlue;
            }
            
            _sisfarma.Clientes.Sincronizar(new List<FAR.Cliente>() { cliente });
        }
    }

    public class VentaMensualActualizacionSincronizadorEmp2 : DC.VentaMensualActualizacionSincronizador
    {
        protected const string TIPO_CLASIFICACION_DEFAULT = "Familia";
        protected const string TIPO_CLASIFICACION_CATEGORIA = "Categoria";
        protected const string SISTEMA_NIXFARMA = "nixfarma";

        private string _clasificacion;
        private bool _debeCopiarClientes;
        private string _copiarClientes;
        private int _anioInicio;
        private string _verCategorias;
        private string _filtrosResidencia;
        private string _codigoEmpresa = "00002";

        public VentaMensualActualizacionSincronizadorEmp2(IFarmaciaService farmacia, ISisfarmaService fisiotes, int listaDeArticulo)
            : base(farmacia, fisiotes, listaDeArticulo)
        { }

        public override void LoadConfiguration()
        {
            base.LoadConfiguration();
            _clasificacion = !string.IsNullOrWhiteSpace(ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION])
                ? ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION]
                : TIPO_CLASIFICACION_DEFAULT;

            _copiarClientes = ConfiguracionPredefinida[Configuracion.FIELD_COPIAS_CLIENTES];
            _debeCopiarClientes = _copiarClientes.ToLower().Equals("si") || string.IsNullOrWhiteSpace(_copiarClientes);
            _anioInicio = ConfiguracionPredefinida[Configuracion.FIELD_ANIO_INICIO]
               .ToIntegerOrDefault(@default: DateTime.Now.Year - 2);
            _verCategorias = ConfiguracionPredefinida[Configuracion.FIELD_VER_CATEGORIAS];
            _puntosDeSisfarma = ConfiguracionPredefinida[Configuracion.FIELD_PUNTOS_SISFARMA];
            _filtrosResidencia = ConfiguracionPredefinida[Configuracion.FIELD_FILTROS_RESIDENCIA];
        }

        public override void Process()
        {
            var cargarPuntosSisfarma = _cargarPuntos == "si";
            var fechaActual = DateTime.Now.Date;
            if (!FechaConfiguracionIsValid(fechaActual, Configuracion.FIELD_POR_DONDE_VOY_VENTA_MES_EMP2))
                return;

            var fechaInicial = CalcularFechaInicialDelProceso(fechaActual);
            if (!_sisfarma.PuntosPendientes.ExistsGreatThanOrEqual(fechaInicial, _codigoEmpresa))
                return;

            var ventaIdConfiguracion = _sisfarma.Configuraciones
                .GetByCampo(Configuracion.FIELD_POR_DONDE_VOY_VENTA_MES_ID_EMP2)
                    .ToIntegerOrDefault();

            var ventas = _farmacia.Ventas.GetAllByIdGreaterOrEqual(ventaIdConfiguracion, fechaInicial, "EMP2");
            var batchPuntosPendientes = new List<PuntosPendientes>();
            foreach (var venta in ventas)
            {
                Task.Delay(5).Wait();
                _cancellationToken.ThrowIfCancellationRequested();

                if (venta.ClienteId > 0)
                    venta.Cliente = _farmacia.Clientes.GetOneOrDefaultById(venta.ClienteId, cargarPuntosSisfarma);

                //venta.VendedorNombre = _farmacia.Vendedores.GetOneOrDefaultById(venta.VendedorId)?.Nombre;
                venta.Detalle = _farmacia.Ventas.GetDetalleDeVentaByVentaId(venta.Operacion, "EMP2");

                if (venta.HasCliente() && _debeCopiarClientes)
                    InsertOrUpdateCliente(venta.Cliente);

                var puntosPendientes = GenerarPuntosPendientes(venta);
                batchPuntosPendientes.AddRange(puntosPendientes);
            }

            if (batchPuntosPendientes.Any()) _sisfarma.PuntosPendientes.Sincronizar(batchPuntosPendientes, calcularPuntos: true);

            _sisfarma.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_VENTA_MES_ID_EMP2, "0");
            _sisfarma.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_VENTA_MES_EMP2, fechaActual.ToString("yyyy-MM-dd"));
        }

        private DateTime CalcularFechaInicialDelProceso(DateTime fechaActual)
        {
            var mesConfiguracion = ConfiguracionPredefinida[Configuracion.FIELD_REVISAR_VENTA_MES_DESDE].ToIntegerOrDefault();
            var mesRevision = (mesConfiguracion > 0) ? -mesConfiguracion : -1;
            return fechaActual.AddMonths(mesRevision);
        }

        private bool FechaConfiguracionIsValid(DateTime fechaActual, string fechaVentaConfiguracion)
        {
            var fechaConfiguracion = _sisfarma.Configuraciones.GetByCampo(fechaVentaConfiguracion).ToDateTimeOrDefault("yyyy-MM-dd");
            return fechaActual.Date != fechaConfiguracion.Date;
        }

        private IEnumerable<PuntosPendientes> GenerarPuntosPendientes(FAR.Venta venta)
        {
            if (!venta.HasDetalle())
                return new PuntosPendientes[0];

            var puntosPendientes = new List<PuntosPendientes>();
            foreach (var item in venta.Detalle.Where(d => d.HasFarmaco()))
            {
                var familia = !string.IsNullOrWhiteSpace(item.Farmaco.Familia?.Nombre) ? item.Farmaco.Familia.Nombre : FAMILIA_DEFAULT;
                var superFamilia = !string.IsNullOrWhiteSpace(item.Farmaco.SuperFamilia?.Nombre) ? item.Farmaco.SuperFamilia.Nombre : FAMILIA_DEFAULT;

                var categoria = item.Farmaco.Categoria?.Nombre;
                if (_verCategorias == "si" && !string.IsNullOrWhiteSpace(categoria) && categoria.ToLower() != "sin categoria" && categoria.ToLower() != "sin categoría")
                {
                    if (string.IsNullOrEmpty(superFamilia) || superFamilia == FAMILIA_DEFAULT)
                        superFamilia = categoria;
                    else superFamilia = $"{superFamilia} ~~~~~~~~ {categoria}";
                }

                var puntoPendiente = new PuntosPendientes
                {
                    VentaId = $"{venta.Operacion}{_codigoEmpresa}".ToLongOrDefault(),
                    LineaNumero = item.Linea,
                    CodigoBarra = item.Farmaco.CodigoBarras ?? "847000" + item.Farmaco.Codigo.PadLeft(6, '0'),
                    CodigoNacional = item.Farmaco.Codigo,
                    Descripcion = item.Farmaco.Denominacion,

                    Familia = familia,
                    SuperFamilia = superFamilia,
                    SuperFamiliaAux = string.Empty,
                    FamiliaAux = string.Empty,
                    CambioClasificacion = _clasificacion == TIPO_CLASIFICACION_CATEGORIA ? 1 : 0,

                    Cantidad = item.Cantidad,
                    Precio = item.Precio,
                    Pago = item.Equals(venta.Detalle.First()) ? venta.TotalBruto : 0,
                    TipoPago = venta.TipoOperacion,
                    Fecha = venta.FechaHora.Date.ToDateInteger(),
                    DNI = venta.Cliente?.Id.ToString() ?? "0",
                    Cargado = _cargarPuntos.ToLower().Equals("si") ? "no" : "si",
                    Puesto = $"{venta.Puesto}",
                    Trabajador = !string.IsNullOrWhiteSpace(venta.VendedorCodigo) ? venta.VendedorCodigo.Trim() : string.Empty,
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
                    Sistema = SISTEMA_NIXFARMA,
                    Ubicacion = item.Farmaco?.Ubicacion
                };

                puntosPendientes.Add(puntoPendiente);
            }

            return puntosPendientes;
        }

        private void InsertOrUpdateCliente(FAR.Cliente cliente)
        {
            var debeCargarPuntos = _puntosDeSisfarma.ToLower().Equals("no") || string.IsNullOrWhiteSpace(_puntosDeSisfarma);
            cliente.DebeCargarPuntos = debeCargarPuntos;

            cliente.Tipo = _farmacia.Clientes.EsResidencia($"{cliente.CodigoCliente}", $"{cliente.CodigoDes}", _filtrosResidencia);

            if (_perteneceFarmazul)
            {
                var beBlue = _farmacia.Clientes.EsBeBlue($"{cliente.CodigoCliente}", $"{cliente.CodigoDes}");
                cliente.BeBlue = beBlue;
            }

            _sisfarma.Clientes.Sincronizar(new List<FAR.Cliente>() { cliente });
        }
    }
}