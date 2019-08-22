using Sisfarma.Sincronizador.Consejo;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Farmatic.Models;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Helpers;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class PuntoPendienteSincronizador : TaskSincronizador
    {
        private const string FAMILIA_DEFAULT = "<Sin Clasificar>";

        private readonly bool _hasSexo;

        private readonly ConsejoService _consejo;

        private bool _perteneceFarmazul;
        private string _puntosDeSisfarma;
        private string _cargarPuntos;
        private string _fechaDePuntos;
        private string _soloPuntosConTarjeta;
        private string _canjeoPuntos;
        private int _anioInicio;
        private long _ultimaVenta;

        public PuntoPendienteSincronizador(FarmaticService farmatic, FisiotesService fisiotes, ConsejoService consejo)
            : base(farmatic, fisiotes)
        {
            _consejo = consejo ?? throw new ArgumentNullException(nameof(consejo));
            _hasSexo = farmatic.Clientes.HasSexoField();
        }

        public override void LoadConfiguration()
        {
            //_perteneceFarmazul = _fisiotes.Configuraciones.PerteneceFarmazul();
            _perteneceFarmazul = bool.Parse(ConfiguracionPredefinida[Configuracion.FIELD_ES_FARMAZUL]);
            _puntosDeSisfarma = ConfiguracionPredefinida[Configuracion.FIELD_PUNTOS_SISFARMA];
            _cargarPuntos = ConfiguracionPredefinida[Configuracion.FIELD_CARGAR_PUNTOS];
            _fechaDePuntos = ConfiguracionPredefinida[Configuracion.FIELD_FECHA_PUNTOS];
            _soloPuntosConTarjeta = ConfiguracionPredefinida[Configuracion.FIELD_SOLO_PUNTOS_CON_TARJETA];
            _canjeoPuntos = ConfiguracionPredefinida[Configuracion.FIELD_CANJEO_PUNTOS];
            _anioInicio = ConfiguracionPredefinida[Configuracion.FIELD_ANIO_INICIO]
                .ToIntegerOrDefault(@default: DateTime.Now.Year - 2);
        }

        public override void PreSincronizacion()
        {
            _ultimaVenta = _fisiotes.PuntosPendientes.GetUltimaVenta();
        }

        public override void Process() => ProcessPuntosPendientes();

        private void ProcessPuntosPendientes()
        {
            var ventas = _farmatic.Ventas.GetByIdGreaterOrEqual(_anioInicio, _ultimaVenta);

            foreach (var venta in ventas)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();
                var dniString = venta.XClie_IdCliente.Strip();
                var dni = dniString.ToIntegerOrDefault();
                var tarjetaDelCliente = string.Empty;

                // TODO: Carga cliente
                if (dni > 0)
                {
                    var cliente = _farmatic.Clientes.GetOneOrDefaulById(dni);
                    tarjetaDelCliente = cliente?.FIS_FAX ?? string.Empty;
                    if (cliente != null)
                        InsertOrUpdateCliente(cliente);
                }

                var vendedor = _farmatic.Vendedores.GetOneOrDefaultById(venta.XVend_IdVendedor)?.NOMBRE ?? "NO";
                var detalleVenta = _farmatic.Ventas.GetLineasVentaByVenta(venta.IdVenta);

                // sólo se carga la desc venta una vez
                var descuentoVentaCargado = false;
                foreach (var linea in detalleVenta)
                {
                    Task.Delay(5);

                    var descuentoVenta = 0d;
                    if (!descuentoVentaCargado)
                    {
                        descuentoVentaCargado = !descuentoVentaCargado;
                        descuentoVenta = venta.DescuentoOpera ?? 0;
                    }

                    //if (!_fisiotes.PuntosPendientes.Exists(venta.IdVenta, linea.IdNLinea))
                    //{
                    //    _fisiotes.PuntosPendientes.Insert(
                    //        GenerarPuntoPendiente(_puntosDeSisfarma, _cargarPuntos, dni, tarjetaDelCliente, descuentoVenta, venta, linea, vendedor, _farmatic, _consejo));
                    //}

                    _fisiotes.PuntosPendientes.Insert(
                        GenerarPuntoPendiente(_puntosDeSisfarma, _cargarPuntos, dni, tarjetaDelCliente, descuentoVenta, venta, linea, vendedor, _farmatic, _consejo));
                }

                // Recuperamos el detalle de ventas virtuales
                //var virtuales = _farmatic.Ventas.GetLineasVirtualesByVenta(venta.IdVenta);
                //foreach (var @virtual in virtuales)
                //{
                //    // Verificamos la entrega del item de venta
                //    if (!_fisiotes.Entregas.Exists(venta.IdVenta, @virtual.IdNLinea))
                //    {
                //        _fisiotes.Entregas.Insert(
                //            GenerarEntregaCliente(venta, @virtual, vendedor));

                //        _fisiotes.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_ENTREGAS_CLIENTES, @virtual.IdVenta.ToString());
                //    }
                //}

                _ultimaVenta = venta.IdVenta;
            }
        }

        private EntregaCliente GenerarEntregaCliente(Venta venta, LineaVentaVirtual lineaVirtual, string vendedor)
        {
            var ec = new EntregaCliente();

            ec.idventa = venta.IdVenta;
            ec.idnlinea = lineaVirtual.IdNLinea;
            ec.codigo = lineaVirtual.Codigo;
            ec.descripcion = lineaVirtual.Descripcion;
            ec.precio = Convert.ToDecimal(lineaVirtual.ImporteNeto);
            ec.tipo = lineaVirtual.TipoLinea;
            ec.fecha = Convert.ToInt32(venta.FechaHora.ToString("yyyyMMdd"));

            if (string.IsNullOrEmpty(venta.XClie_IdCliente.Strip()?.Trim()) || string.IsNullOrWhiteSpace(venta.XClie_IdCliente.Strip()))
                ec.dni = "0";
            else
                ec.dni = venta.XClie_IdCliente.Strip();

            ec.puesto = venta.Maquina;
            ec.trabajador = vendedor;
            ec.pvp = Convert.ToSingle(lineaVirtual.Pvp);
            ec.fechaEntrega = venta.FechaHora;

            return ec;
        }

        private PuntosPendientes GenerarPuntoPendiente(string puntosDeSisfarma, string cargarPuntos, int dni, string tarjetaDelCliente, double descuentoVenta, Venta venta, LineaVenta linea, string vendedor, FarmaticService farmatic, ConsejoService consejo)
        {
            var redencion = (farmatic.Ventas.GetOneOrDefaultLineaRedencionByKey(venta.IdVenta, linea.IdNLinea)?
                .Redencion) ?? 0;

            var articulo = farmatic.Articulos.GetOneOrDefaultById(linea.Codigo);
            var pp = new PuntosPendientes();
            pp.idventa = venta.IdVenta;
            pp.idnlinea = linea.IdNLinea;
            pp.puntos = 0;
            pp.puesto = venta.Maquina;
            pp.tipoPago = linea.TipoLinea;
            pp.fechaVenta = venta.FechaHora;

            if (string.IsNullOrEmpty(venta.XClie_IdCliente.Strip()?.Trim()) || string.IsNullOrWhiteSpace(venta.XClie_IdCliente.Strip()))
                pp.dni = "0";
            else
                pp.dni = venta.XClie_IdCliente.Strip();

            pp.trabajador = vendedor;
            pp.fecha = Convert.ToInt32(venta.FechaHora.ToString("yyyyMMdd"));
            pp.recetaPendiente = linea.RecetaPendiente;
            pp.receta = linea.TipoAportacion;
            pp.redencion = Convert.ToSingle(redencion);
            pp.cod_nacional = linea.Codigo;
            pp.cod_barras = GetCodidoBarrasFromLocalOrDefault(farmatic, linea.Codigo);
            pp.descripcion = linea.Descripcion.Strip();
            pp.pvp = Convert.ToSingle(linea.PVP);
            pp.dtoVenta = Convert.ToSingle(descuentoVenta);
            pp.dtoLinea = Convert.ToSingle(linea.DescuentoLinea ?? 0d);
            pp.precio = Convert.ToDecimal(linea.ImporteNeto);
            pp.cantidad = linea.Cantidad;
            pp.cargado = cargarPuntos.ToLower().Equals("si") ? "no" : "si";

            if (articulo == null)
            {
                pp.laboratorio = "<Sin Laboratorio>";
                pp.cod_laboratorio = string.Empty;
                pp.familia = FAMILIA_DEFAULT;
                pp.superFamilia = FAMILIA_DEFAULT;
                pp.proveedor = string.Empty;
                pp.puc = 0;
            }
            else
            {
                pp.cod_laboratorio = articulo.Laboratorio.Strip() ?? string.Empty;
                pp.laboratorio = GetNombreLaboratorioFromLocalOrDefault(farmatic, consejo, pp.cod_laboratorio, "<Sin Laboratorio>");
                pp.puc = Convert.ToSingle(articulo.Puc);
                pp.familia = GetFamiliaFromLocalOrDefault(farmatic, articulo.XFam_IdFamilia, "<Sin Clasificar>");
                pp.superFamilia = !pp.familia.Equals("<Sin Clasificar>")
                    ? GetSuperFamiliaFromLocalOrDefault(farmatic, pp.familia, "<Sin Clasificar>").Strip()
                    : pp.familia.Strip();

                pp.familia = pp.familia.Strip();

                pp.proveedor = GetProveedorFromLocalOrDefault(farmatic, articulo.IdArticu).Strip();
            }

            //var sonpuntosdesisfarma = puntosdesisfarma.tolower().equals("si");
            //var fechadeventa = venta.fechahora.date;
            //var cargado = cargarpuntos.tolower().equals("si");

            //if (dni != 0 &&
            //    sonpuntosdesisfarma && !cargado &&
            //    !string.isnullorwhitespace(_fechadepuntos) &&
            //    _fechadepuntos.tolower() != "no" &&
            //    fechadeventa >= _fechadepuntos.todatetimeordefault("yyyymmdd"))
            //{
            //    var tipofamilia = pp.familia != familia_default ? pp.familia : pp.superfamilia;
            //    var importe = linea.importeneto;
            //    var articulodescripcion = articulo?.descripcion ?? string.empty;
            //    var articulocantidad = linea.cantidad;

            //    pp.puntos = (float)calcularpuntos(tarjetadelcliente, tipofamilia, importe, articulodescripcion, articulocantidad);
            //}
            //else if (dni != 0 && _fechadepuntos.tolower() != "no")
            //    pp.cargado = "no";

            return pp;
        }

        private string GetCodidoBarrasFromLocalOrDefault(FarmaticService farmaticService, string articulo)
        {
            var sinonimo = farmaticService.Sinonimos.GetOneOrDefaultByArticulo(articulo);
            return sinonimo?.Sinonimo ?? "847000" + articulo.PadLeft(6, '0');
        }

        private string GetFamiliaFromLocalOrDefault(FarmaticService farmatic, short id, string byDefault = "")
        {
            var familiaDb = farmatic.Familias.GetById(id);
            return !string.IsNullOrEmpty(familiaDb?.Descripcion)
                ? familiaDb.Descripcion
                : byDefault;
        }

        private string GetSuperFamiliaFromLocalOrDefault(FarmaticService farmaticService, string familia, string byDefault = "")
        {
            var superfamilia = farmaticService.Familias.GetSuperFamiliaDescripcionByFamilia(familia) ?? byDefault;
            return superfamilia;
        }

        private string GetNombreLaboratorioFromLocalOrDefault(FarmaticService farmaticService, ConsejoService consejoService, string codigo, string byDefault = "")
        {
            var nombreLaboratorio = byDefault;
            if (!string.IsNullOrEmpty(codigo?.Trim()) && !string.IsNullOrWhiteSpace(codigo))
            {
                var laboratorioDb = default(Consejo.Models.Labor); //consejoService.Laboratorios.Get(codigo);
                if (laboratorioDb == null)
                {
                    var laboratorioLocal =
                        farmaticService.Laboratorios.GetById(codigo);
                    nombreLaboratorio = laboratorioLocal?.Nombre ?? byDefault;
                }
                else nombreLaboratorio = laboratorioDb.NOMBRE;
            }
            else nombreLaboratorio = byDefault;

            return nombreLaboratorio;
        }

        private string GetProveedorFromLocalOrDefault(FarmaticService farmaticService, string codigoNacional, string byDefault = "")
        {
            var proveedorDb = farmaticService.Proveedores.GetOneOrDefaultByCodigoNacional(codigoNacional);
            return proveedorDb?.FIS_NOMBRE ?? byDefault;
        }

        private void InsertOrUpdateCliente(Farmatic.Models.Cliente cliente)
        {
            var clienteDTO = Generator.GenerarCliente(_farmatic, cliente, _hasSexo);

            var puntosDeSisfarma = _puntosDeSisfarma;
            var debeCargarPuntos = puntosDeSisfarma.ToLower().Equals("no") || string.IsNullOrWhiteSpace(puntosDeSisfarma);

            var dniCliente = cliente.PER_NIF.Strip();
            if (_perteneceFarmazul)
            {
                var beBlue = _farmatic.Clientes.EsBeBlue(cliente.XTIPO_IDTIPO) ? 1 : 0;

                if (debeCargarPuntos)
                {
                    _fisiotes.Clientes.InsertOrUpdateBeBlue(
                    clienteDTO.Trabajador, clienteDTO.Tarjeta, cliente.IDCLIENTE, dniCliente, clienteDTO.Nombre.Strip(), clienteDTO.Telefono, clienteDTO.Direccion.Strip(),
                    clienteDTO.Movil, clienteDTO.Email, clienteDTO.Puntos, clienteDTO.FechaNacimiento, clienteDTO.Sexo, clienteDTO.FechaAlta, clienteDTO.Baja, clienteDTO.Lopd,
                    beBlue);
                }
                else
                {
                    _fisiotes.Clientes.InsertOrUpdateBeBlue(
                        clienteDTO.Trabajador, clienteDTO.Tarjeta, cliente.IDCLIENTE, dniCliente, clienteDTO.Nombre.Strip(), clienteDTO.Telefono, clienteDTO.Direccion.Strip(),
                        clienteDTO.Movil, clienteDTO.Email, clienteDTO.FechaNacimiento, clienteDTO.Sexo, clienteDTO.FechaAlta, clienteDTO.Baja, clienteDTO.Lopd,
                        beBlue);
                }
            }
            else if (debeCargarPuntos)
            {
                _fisiotes.Clientes.InsertOrUpdate(
                    clienteDTO.Trabajador, clienteDTO.Tarjeta, cliente.IDCLIENTE, dniCliente, clienteDTO.Nombre.Strip(), clienteDTO.Telefono, clienteDTO.Direccion.Strip(),
                    clienteDTO.Movil, clienteDTO.Email, clienteDTO.Puntos, clienteDTO.FechaNacimiento, clienteDTO.Sexo, clienteDTO.FechaAlta, clienteDTO.Baja, clienteDTO.Lopd);
            }
            else
            {
                _fisiotes.Clientes.InsertOrUpdate(
                    clienteDTO.Trabajador, clienteDTO.Tarjeta, cliente.IDCLIENTE, dniCliente, clienteDTO.Nombre.Strip(), clienteDTO.Telefono, clienteDTO.Direccion.Strip(),
                    clienteDTO.Movil, clienteDTO.Email, clienteDTO.FechaNacimiento, clienteDTO.Sexo, clienteDTO.FechaAlta, clienteDTO.Baja, clienteDTO.Lopd);
            }
        }

        //private double CalcularPuntos(string tarjetaDelCliente, string tipoFamilia, double importe, string articuloDescripcion, int articuloCantidad)
        //{
        //    var puntos = importe *
        //        (double)(_fisiotes.Familias.GetPuntosByFamiliaTipoVerificado(tipoFamilia));

        //    if (_soloPuntosConTarjeta.ToLower() == "si" && string.IsNullOrWhiteSpace(tarjetaDelCliente))
        //        puntos = 0;

        //    if (_canjeoPuntos.ToLower() != "si" && articuloDescripcion.Contains("FIDELIZACION"))
        //        puntos = Math.Abs(articuloCantidad) * -1;

        //    return puntos;
        //}
    }
}