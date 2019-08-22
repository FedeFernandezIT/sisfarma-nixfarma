using Sisfarma.Sincronizador.Consejo;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Farmatic.Models;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.DTO.Clientes;
using Sisfarma.Sincronizador.Fisiotes.DTO.PuntosPendientes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Helpers;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System;
using System.Threading.Tasks;
using static Sisfarma.Sincronizador.Fisiotes.Repositories.ConfiguracionesRepository;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class VentaMensualActualizacionSincronizador : TaskSincronizador
    {        
        private bool _perteneceFarmazul;
        private string _puntosDeSisfarma;
        private string _cargarPuntos;
        private string _fechaDePuntos;
        private string _soloPuntosConTarjeta;
        private string _canjeoPuntos;

        private const string VENDEDOR_DEFAULT = "NO";
        private const string COD_BARRAS_DEFAULT = "847000";
        private const string LABORATORIO_DEAFULT = "<Sin Laboratorio>";
        private const string FAMILIA_DEFAULT = "<Sin Clasificar>";

        private readonly ConsejoService _consejo;
        private readonly int _listaDeArticulo;
        private readonly bool _hasSexo;

        public VentaMensualActualizacionSincronizador(FarmaticService farmatic, FisiotesService fisiotes, ConsejoService consejo, int listaDeArticulo)
            : base(farmatic, fisiotes)
        {
            _consejo = consejo ?? throw new ArgumentNullException(nameof(consejo));
            _listaDeArticulo = listaDeArticulo;
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
        }

        public override void Process() => ProcessVentaMensual();

        private void ProcessVentaMensual()
        {
            var fechaActual = DateTime.Now.Date;
            if (!FechaConfiguracionIsValid(fechaActual))
                return;

            var fechaInicial = CalcularFechaInicialDelProceso(fechaActual);
            if (!_fisiotes.PuntosPendientes.ExistsGreatThanOrEqual(fechaInicial))
                return;

            //var ventaIdConfiguracion = ConfiguracionPredefinida[Configuracion.FIELD_POR_DONDE_VOY_VENTA_MES_ID].ToIntegerOrDefault();
            var ventaIdConfiguracion = _fisiotes.Configuraciones
                .GetByCampo(Configuracion.FIELD_POR_DONDE_VOY_VENTA_MES_ID)
                    .ToIntegerOrDefault();

            var ventas = _farmatic.Ventas.GetGreatThanOrEqual(ventaIdConfiguracion, fechaInicial);
            foreach (var venta in ventas)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                var dniString = "";

                if (string.IsNullOrEmpty(venta.XClie_IdCliente.Strip()?.Trim()) || string.IsNullOrWhiteSpace(venta.XClie_IdCliente.Strip()))
                    dniString = "0";
                else
                    dniString = venta.XClie_IdCliente.Strip();

                var dni = dniString.ToIntegerOrDefault();
                var tarjetaDelCliente = string.Empty;
                if (dni > 0)
                {
                    var cliente = _farmatic.Clientes.GetOneOrDefaulById(dni);
                    tarjetaDelCliente = cliente?.FIS_FAX ?? string.Empty;
                    if (cliente != null)
                        InsertOrUpdateCliente(cliente);
                }

                var vendedor = _farmatic.Vendedores.GetOneOrDefaultById(venta.XVend_IdVendedor)?.NOMBRE.Strip()
                    ?? VENDEDOR_DEFAULT;

                var fechaDeVenta = venta.FechaHora.Date;

                //var fechaDePuntos = _fisiotes.Configuraciones.GetByCampo(FIELD_FECHA_PUNTOS);
                //var cargarPuntos = _fisiotes.Configuraciones.GetByCampo(FIELD_CARGAR_PUNTOS) ?? string.Empty;
                //var cargado = cargarPuntos.ToLower().Equals("si");
                //var puntosDeSisfarma = _fisiotes.Configuraciones.GetByCampo(FIELD_PUNTOS_SISFARMA) ?? string.Empty;
                //var sonPuntosDeSisfarma = puntosDeSisfarma.ToLower().Equals("si");

                //var newInsert = false;

                var detalleDeVenta = _farmatic.Ventas.GetLineasVentaByVenta(venta.IdVenta);
                foreach (var item in detalleDeVenta)
                {
                    Task.Delay(5);
                    //var puntoPendiente = _fisiotes.PuntosPendientes.GetOneOrDefaultByItemVenta(item.IdVenta, item.IdNLinea);
                    //if (puntoPendiente == null)
                    //{
                    var articulo = _farmatic.Articulos.GetOneOrDefaultById(item.Codigo);
                        var pp = GenerarPuntoPendienteCargadoPorDefault(dni, venta, item, articulo, vendedor);

                        //if (dni != 0 &&
                        //    sonPuntosDeSisfarma && !cargado &&
                        //    !string.IsNullOrWhiteSpace(fechaDePuntos) &&
                        //    fechaDePuntos.ToLower() != "no" &&
                        //    fechaDeVenta >= fechaDePuntos.ToDateTimeOrDefault("yyyyMMdd"))
                        //{
                        //    var tipoFamilia = pp.familia != FAMILIA_DEFAULT ? pp.familia : pp.superFamilia;
                        //    var importe = item.ImporteNeto;
                        //    var articuloDescripcion = articulo?.Descripcion ?? string.Empty;
                        //    var articuloCantidad = item.Cantidad;

                        //    pp.puntos = (float)CalcularPuntos(tarjetaDelCliente, tipoFamilia, importe, articuloDescripcion, articuloCantidad);
                        //}
                        //else if (dni != 0 && fechaDePuntos.ToLower() != "no")
                        //    pp.cargado = "no";

                        _fisiotes.PuntosPendientes.InsertPuntuacion(pp);
                        //newInsert = true;
                    //}
                    //else
                    //{
                    //    if ((string.IsNullOrWhiteSpace(cargarPuntos) || cargarPuntos.ToLower() == "no") ||
                    //        (cargarPuntos.ToLower() == "si" && puntoPendiente.dni == "0"))
                    //    {
                    //        if (HayDiferencias(dni, vendedor, puntoPendiente, item))
                    //            _fisiotes.PuntosPendientes.UpdatePuntacion(GenerarUpdatePuntoPendiente(dni, venta, item, vendedor));
                    //    }
                    //}
                }

                //if (newInsert &&
                //    dni != 0 &&
                //    sonPuntosDeSisfarma && !cargado &&
                //    fechaDePuntos.ToLower() != "no" &&
                //    !string.IsNullOrWhiteSpace(fechaDePuntos) &&
                //    fechaDeVenta >= fechaDePuntos.ToDateTimeOrDefault("yyyyMMdd"))
                //{
                //    var puntosDelCliente = Math.Round(_fisiotes.PuntosPendientes.GetPuntosByDni(dni), 2);

                //    var puntosCanjeadosDelCliente = Math.Round(_fisiotes.PuntosPendientes.GetPuntosCanjeadosByDni(dni), 2);

                //    var puntosCalculados = Math.Round(puntosDelCliente - puntosCanjeadosDelCliente, 2);

                //    _fisiotes.Clientes.UpdatePuntos(new UpdatePuntaje { dni = dniString, puntos = puntosCalculados });
                //}

                //_fisiotes.Configuraciones.Update(FIELD_POR_DONDE_VOY_VENTA_MES_ID, $"{venta.IdVenta}");
            }

            _fisiotes.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_VENTA_MES_ID, "0");
            _fisiotes.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_VENTA_MES, fechaActual.ToString("yyyy-MM-dd"));
        }

        private DateTime CalcularFechaInicialDelProceso(DateTime fechaActual)
        {
            var mesConfiguracion = ConfiguracionPredefinida[Configuracion.FIELD_REVISAR_VENTA_MES_DESDE].ToIntegerOrDefault();
            var mesRevision = (mesConfiguracion > 0) ? -mesConfiguracion : -1;
            return fechaActual.AddMonths(mesRevision);
        }

        private bool FechaConfiguracionIsValid(DateTime fechaActual)
        {
            //var fechaConfiguracion = ConfiguracionPredefinida[Configuracion.FIELD_POR_DONDE_VOY_VENTA_MES].ToDateTimeOrDefault("yyyy-MM-dd");
            var fechaConfiguracion = _fisiotes.Configuraciones.GetByCampo(Configuracion.FIELD_POR_DONDE_VOY_VENTA_MES).ToDateTimeOrDefault("yyyy-MM-dd");


            return fechaActual.Date != fechaConfiguracion.Date;
        }

        private UpdatePuntacion GenerarUpdatePuntoPendiente(int dniDelCliente, Venta venta, LineaVenta detalleDeVenta, string vendedor)
        {
            return new UpdatePuntacion
            {
                idventa = detalleDeVenta.IdVenta,
                idnlinea = detalleDeVenta.IdNLinea,
                cantidad = detalleDeVenta.Cantidad,
                precio = (decimal)detalleDeVenta.ImporteNeto,
                tipoPago = detalleDeVenta.TipoLinea,
                dni = dniDelCliente.ToString(),
                trabajador = vendedor,
                receta = detalleDeVenta.TipoAportacion,
                dtoLinea = (float)(detalleDeVenta.DescuentoLinea ?? 0),
                dtoVenta = (float)(detalleDeVenta.DescuentoOpera ?? 0)
            };
        }

        //private double CalcularPuntos(string tarjetaDelCliente, string tipoFamilia, double importe, string articuloDescripcion, int articuloCantidad)
        //{
        //    var puntos = importe *
        //        (double)(_fisiotes.Familias.GetPuntosByFamiliaTipoVerificado(tipoFamilia));

        //    var soloPuntosConTarjeta = _fisiotes.Configuraciones.GetByCampo(FIELD_SOLO_PUNTOS_CON_TARJETA);
        //    if (soloPuntosConTarjeta.ToLower() == "si" && string.IsNullOrWhiteSpace(tarjetaDelCliente))
        //        puntos = 0;

        //    var canjeoPuntos = _fisiotes.Configuraciones.GetByCampo(FIELD_CANJEO_PUNTOS);
        //    if (canjeoPuntos.ToLower() != "si" && articuloDescripcion.Contains("FIDELIZACION"))
        //        puntos = Math.Abs(articuloCantidad) * -1;

        //    return puntos;
        //}

        private InsertPuntuacion GenerarPuntoPendienteCargadoPorDefault(int dniDelCliente, Venta venta, LineaVenta detalleDeVenta, Articulo articulo, string vendedor)
        {
            var puntos = 0d;
            var cargado = "si";

            var familia = FAMILIA_DEFAULT;
            var superFamilia = FAMILIA_DEFAULT;
            var codLaboratorio = string.Empty;
            var pvp = 0f;
            var puc = 0f;
            if (articulo != null)
            {
                familia = _farmatic.Familias.GetById(articulo.XFam_IdFamilia)?.Descripcion;
                if (string.IsNullOrEmpty(familia))
                    familia = FAMILIA_DEFAULT;

                superFamilia = !familia.Equals(FAMILIA_DEFAULT)
                    ? _farmatic.Familias.GetSuperFamiliaDescripcionByFamilia(familia) ?? FAMILIA_DEFAULT
                    : familia;

                pvp = (float)articulo.Pvp;
                puc = (float)articulo.Puc;

                codLaboratorio = articulo.Laboratorio.Strip() ?? string.Empty;
            }

            var redencion = _farmatic.Ventas
                            .GetOneOrDefaultLineaRedencionByKey(detalleDeVenta.IdVenta, detalleDeVenta.IdNLinea)?.Redencion
                                ?? 0;

            var codigoBarra = _farmatic.Sinonimos
                            .GetOneOrDefaultByArticulo(detalleDeVenta.Codigo)?.Sinonimo.Strip()
                                ?? COD_BARRAS_DEFAULT;

            var proveedor = _farmatic.Proveedores.GetOneOrDefaultByCodigoNacional(detalleDeVenta.Codigo)?.FIS_NOMBRE.Strip() ?? string.Empty;

            var laboratorio = Generator.GetNombreLaboratorioFromLocalOrDefault(_farmatic, _consejo, codLaboratorio, LABORATORIO_DEAFULT).Strip();

            return new InsertPuntuacion
            {
                idventa = detalleDeVenta.IdVenta,
                idnlinea = detalleDeVenta.IdNLinea,
                cod_barras = codigoBarra,
                cod_nacional = detalleDeVenta.Codigo.Strip(),
                descripcion = detalleDeVenta.Descripcion.Strip(),
                familia = familia.Strip(),
                superFamilia = superFamilia.Strip(),
                cantidad = detalleDeVenta.Cantidad,
                precio = (decimal)detalleDeVenta.ImporteNeto,
                tipoPago = detalleDeVenta.TipoLinea,
                fecha = venta.FechaHora.ToDateInteger(),
                dni = dniDelCliente.ToString(),
                cargado = cargado,
                puesto = venta.Maquina,
                trabajador = vendedor,
                cod_laboratorio = codLaboratorio,
                laboratorio = laboratorio,
                proveedor = proveedor,
                receta = detalleDeVenta.TipoAportacion,
                fechaVenta = venta.FechaHora,
                pvp = pvp,
                puc = puc,
                puntos = (float)puntos,
                dtoLinea = (float)(detalleDeVenta.DescuentoLinea ?? 0),
                dtoVenta = (float)(detalleDeVenta.DescuentoOpera ?? 0),
                redencion = (float)redencion,
                recetaPendiente = detalleDeVenta.RecetaPendiente,
                actualizado = 1
            };
        }

        private bool HayDiferencias(int dni, string vendedor, PuntosPendientes remoto, LineaVenta local)
        {
            return
                local.Cantidad != remoto.cantidad ||
                dni != remoto.dni.ToIntegerOrDefault() ||
                Math.Round(local.ImporteNeto, 2) != (double)Math.Round(remoto.precio, 2) ||
                Math.Round(local.DescuentoLinea.GetValueOrDefault(), 2) != Math.Round(remoto.dtoLinea.GetValueOrDefault(), 2) ||
                Math.Round(local.DescuentoOpera.GetValueOrDefault(), 2) != Math.Round(remoto.dtoVenta.GetValueOrDefault(), 2) ||
                vendedor.Trim().ToUpper() != remoto.trabajador.Trim().ToUpper() ||
                local.TipoAportacion.Trim().ToUpper() != remoto.receta.Trim().ToUpper();
        }

        private void InsertOrUpdateCliente(Farmatic.Models.Cliente cliente)
        {
            var clienteDTO = Generator.GenerarCliente(_farmatic, cliente, _hasSexo);

            //var puntosDeSisfarma = _fisiotes.Configuraciones.GetByCampo(FIELD_PUNTOS_SISFARMA) ?? string.Empty;
            var debeCargarPuntos = _puntosDeSisfarma.ToLower().Equals("no") || string.IsNullOrWhiteSpace(_puntosDeSisfarma);

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
    }
}