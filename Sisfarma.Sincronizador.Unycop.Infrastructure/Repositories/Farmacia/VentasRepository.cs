using Oracle.DataAccess.Client;
using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Linq;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public class VentasRepository : FarmaciaRepository, IVentasRepository
    {
        private readonly IClientesRepository _clientesRepository;
        private readonly ITicketRepository _ticketRepository;
        private readonly IVendedoresRepository _vendedoresRepository;
        private readonly IFarmacoRepository _farmacoRepository;
        private readonly ICodigoBarraRepository _barraRepository;
        private readonly IProveedorRepository _proveedorRepository;
        private readonly ICategoriaRepository _categoriaRepository;
        private readonly IFamiliaRepository _familiaRepository;
        private readonly ILaboratorioRepository _laboratorioRepository;

        private readonly decimal _factorCentecimal = 0.01m;

        public VentasRepository(LocalConfig config,
            IClientesRepository clientesRepository,
            ITicketRepository ticketRepository,
            IVendedoresRepository vendedoresRepository,
            IFarmacoRepository farmacoRepository,
            ICodigoBarraRepository barraRepository,
            IProveedorRepository proveedorRepository,
            ICategoriaRepository categoriaRepository,
            IFamiliaRepository familiaRepository,
            ILaboratorioRepository laboratorioRepository) : base(config)
        {
            _clientesRepository = clientesRepository ?? throw new ArgumentNullException(nameof(clientesRepository));
            _ticketRepository = ticketRepository ?? throw new ArgumentNullException(nameof(ticketRepository));
            _farmacoRepository = farmacoRepository ?? throw new ArgumentNullException(nameof(farmacoRepository));
            _barraRepository = barraRepository ?? throw new ArgumentNullException(nameof(barraRepository));
            _proveedorRepository = proveedorRepository ?? throw new ArgumentNullException(nameof(proveedorRepository));
            _categoriaRepository = categoriaRepository ?? throw new ArgumentNullException(nameof(categoriaRepository));
            _familiaRepository = familiaRepository ?? throw new ArgumentNullException(nameof(familiaRepository));
            _laboratorioRepository = laboratorioRepository ?? throw new ArgumentNullException(nameof(laboratorioRepository));
            _vendedoresRepository = vendedoresRepository ?? throw new ArgumentNullException(nameof(vendedoresRepository));
        }

        public VentasRepository(
            IClientesRepository clientesRepository,
            ITicketRepository ticketRepository,
            IVendedoresRepository vendedoresRepository,
            IFarmacoRepository farmacoRepository,
            ICodigoBarraRepository barraRepository,
            IProveedorRepository proveedorRepository,
            ICategoriaRepository categoriaRepository,
            IFamiliaRepository familiaRepository,
            ILaboratorioRepository laboratorioRepository)
        {
            _clientesRepository = clientesRepository ?? throw new ArgumentNullException(nameof(clientesRepository));
            _ticketRepository = ticketRepository ?? throw new ArgumentNullException(nameof(ticketRepository));
            _farmacoRepository = farmacoRepository ?? throw new ArgumentNullException(nameof(farmacoRepository));
            _barraRepository = barraRepository ?? throw new ArgumentNullException(nameof(barraRepository));
            _proveedorRepository = proveedorRepository ?? throw new ArgumentNullException(nameof(proveedorRepository));
            _categoriaRepository = categoriaRepository ?? throw new ArgumentNullException(nameof(categoriaRepository));
            _familiaRepository = familiaRepository ?? throw new ArgumentNullException(nameof(familiaRepository));
            _laboratorioRepository = laboratorioRepository ?? throw new ArgumentNullException(nameof(laboratorioRepository));
            _vendedoresRepository = vendedoresRepository ?? throw new ArgumentNullException(nameof(vendedoresRepository));
        }

        public Venta GetOneOrDefaultById(long id)
        {
            var year = int.Parse($"{id}".Substring(0, 4));
            var ventaId = int.Parse($"{id}".Substring(4));

            DTO.Venta ventaAccess;
            try
            {
                using (var db = FarmaciaContext.VentasByYear(year))
                {
                    var sql = @"SELECT ID_VENTA as Id, Fecha, NPuesto as Puesto, Cliente, Vendedor, Descuento, Pago, Tipo, Importe FROM ventas WHERE ID_VENTA = @id";
                    ventaAccess = db.Database.SqlQuery<DTO.Venta>(sql,
                        new OleDbParameter("id", ventaId))
                        .FirstOrDefault();
                }
            }
            catch (FarmaciaContextException)
            {
                ventaAccess = null;
            }

            if (ventaAccess == null)
                return null;

            var venta = new Venta
            {
                Id = ventaAccess.Id,
                Tipo = ventaAccess.Tipo.ToString(),
                FechaHora = ventaAccess.Fecha,
                Puesto = ventaAccess.Puesto.ToString(),
                ClienteId = ventaAccess.Cliente,
                VendedorId = ventaAccess.Vendedor,
                TotalDescuento = ventaAccess.Descuento * _factorCentecimal,
                TotalBruto = ventaAccess.Pago * _factorCentecimal,
                Importe = ventaAccess.Importe * _factorCentecimal,
            };

            if (ventaAccess.Cliente > 0)
                venta.Cliente = _clientesRepository.GetOneOrDefaultById(ventaAccess.Cliente, false); // TODO verifca hacer

            var ticket = _ticketRepository.GetOneOrdefaultByVentaId(ventaAccess.Id, year);
            if (ticket != null)
            {
                venta.Ticket = new Ticket
                {
                    Numero = ticket.Numero,
                    Serie = ticket.Serie
                };
            }

            venta.VendedorNombre = _vendedoresRepository.GetOneOrDefaultById(ventaAccess.Vendedor)?.Nombre;
            venta.Detalle = GetDetalleDeVentaByVentaId(year, ventaAccess.Id);

            return venta;
        }

        public List<Venta> GetAllByIdGreaterOrEqual(int year, long value)
        {
            try
            {
                var sql = $@"SELECT
                                FECHA_VENTA, FECHA_FIN, CLI_CODIGO, TIPO_OPERACION, OPERACION, PUESTO, USR_CODIGO, IMPORTE_VTA_E, EMP_CODIGO
                                FROM appul.ah_ventas
                                WHERE ROWNUM <= 999
                                    AND emp_codigo = 'EMP1'
                                    AND situacion = 'N'
                                    AND fecha_venta >= to_date('01/01/{year}', 'DD/MM/YYYY')
                                    AND fecha_venta >= to_date('01/01/{year} 00:00:00', 'DD/MM/YYYY HH24:MI:SS')
                                    ORDER BY fecha_venta ASC";

                string connectionString = @"User Id=""CONSU""; Password=""consu"";" +
                @"Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=IPC)(KEY=DP9))" +
                    "(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.0.30)(PORT=1521)))(CONNECT_DATA=(INSTANCE_NAME=DP9)(SERVICE_NAME=ORACLE9)))";

                var conn = new OracleConnection(connectionString);
                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                var ventas = new List<Venta>();
                while (reader.Read())
                {
                    var fechaVenta = Convert.ToDateTime(reader["FECHA_VENTA"]);
                    var fechaFin = !Convert.IsDBNull(reader["FECHA_FIN"]) ? (DateTime?)Convert.ToDateTime(reader["FECHA_FIN"]) : null;
                    var cliCodigo = !Convert.IsDBNull(reader["CLI_CODIGO"]) ? (long)Convert.ToInt32(reader["CLI_CODIGO"]) : 0;
                    var tipoOperacion = Convert.ToString(reader["TIPO_OPERACION"]);
                    var operacion = Convert.ToInt64(reader["OPERACION"]);
                    var puesto = Convert.ToString(reader["PUESTO"]);
                    var usrCodigo = Convert.ToString(reader["USR_CODIGO"]);
                    var importeVentaE = !Convert.IsDBNull(reader["IMPORTE_VTA_E"]) ? Convert.ToDecimal(reader["IMPORTE_VTA_E"]) : default(decimal);
                    var empCodigo = Convert.ToString(reader["EMP_CODIGO"]);
                    ventas.Add(new Venta
                    {
                        ClienteId = cliCodigo,
                        FechaFin = fechaFin,
                        FechaHora = fechaVenta,
                        TipoOperacion = tipoOperacion,
                        Operacion = operacion,
                        Puesto = puesto,
                        VendedorCodigo = usrCodigo,
                        Importe = importeVentaE,
                        EmpresaCodigo = empCodigo
                    });
                }

                conn.Close();
                return ventas;
            }
            catch (Exception ex)
            {
                return new List<Venta>();
            }
        }

        public List<Venta> GetAllByIdGreaterOrEqual(int year, DateTime timestamp)
        {
            try
            {
                var dateTimeFormated = timestamp.ToString("dd/MM/yyyy HH:mm:ss");
                var sql = $@"SELECT
                                FECHA_VENTA, FECHA_FIN, CLI_CODIGO, TIPO_OPERACION, OPERACION, PUESTO, USR_CODIGO, IMPORTE_VTA_E, EMP_CODIGO
                                FROM appul.ah_ventas
                                WHERE ROWNUM <= 999
                                    AND emp_codigo = 'EMP1'
                                    AND situacion = 'N'
                                    AND fecha_venta >= to_date('01/01/{year}', 'DD/MM/YYYY')
                                    AND fecha_venta >= to_date('{dateTimeFormated}', 'DD/MM/YYYY HH24:MI:SS')
                                    ORDER BY fecha_venta ASC";

                string connectionString = @"User Id=""CONSU""; Password=""consu"";" +
                @"Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=IPC)(KEY=DP9))" +
                    "(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.0.30)(PORT=1521)))(CONNECT_DATA=(INSTANCE_NAME=DP9)(SERVICE_NAME=ORACLE9)))";

                var conn = new OracleConnection(connectionString);
                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                var ventas = new List<Venta>();
                while (reader.Read())
                {
                    var fechaVenta = Convert.ToDateTime(reader["FECHA_VENTA"]);
                    var fechaFin = !Convert.IsDBNull(reader["FECHA_FIN"]) ? (DateTime?)Convert.ToDateTime(reader["FECHA_FIN"]) : null;
                    var cliCodigo = !Convert.IsDBNull(reader["CLI_CODIGO"]) ? (long)Convert.ToInt32(reader["CLI_CODIGO"]) : 0;
                    var tipoOperacion = Convert.ToString(reader["TIPO_OPERACION"]);
                    var operacion = Convert.ToInt64(reader["OPERACION"]);
                    var puesto = Convert.ToString(reader["PUESTO"]);
                    var usrCodigo = Convert.ToString(reader["USR_CODIGO"]);
                    var importeVentaE = !Convert.IsDBNull(reader["IMPORTE_VTA_E"]) ? Convert.ToDecimal(reader["IMPORTE_VTA_E"]) : default(decimal);
                    var empCodigo = Convert.ToString(reader["EMP_CODIGO"]);
                    ventas.Add(new Venta
                    {
                        ClienteId = cliCodigo,
                        FechaFin = fechaFin,
                        FechaHora = fechaVenta,
                        TipoOperacion = tipoOperacion,
                        Operacion = operacion,
                        Puesto = puesto,
                        VendedorCodigo = usrCodigo,
                        Importe = importeVentaE,
                        EmpresaCodigo = empCodigo
                    });
                }

                conn.Close();
                return ventas;
            }
            catch (Exception ex)
            {
                return new List<Venta>();
            }
        }

        private Venta GenerarVentaEncabezado(DTO.Venta venta)
            => new Venta
            {
                Id = venta.Id,
                Tipo = venta.Tipo.ToString(),
                FechaHora = venta.Fecha,
                Puesto = venta.Puesto.ToString(),
                ClienteId = venta.Cliente,
                VendedorId = venta.Vendedor,
                TotalDescuento = venta.Descuento * _factorCentecimal,
                TotalBruto = venta.Pago * _factorCentecimal,
                Importe = venta.Importe * _factorCentecimal,
            };

        public List<Venta> GetAllByIdGreaterOrEqual(long id, DateTime fecha)
        {
            var rs = new List<DTO.Venta>();
            try
            {
                using (var db = FarmaciaContext.VentasByYear(fecha.Year))
                {
                    var year = fecha.Year;
                    var fechaInicial = fecha.Date.ToString("dd-MM-yyyy HH:mm:ss");

                    var sql = $@"SELECT TOP 999 ID_VENTA as Id, Fecha, NPuesto as Puesto, Cliente, Vendedor, Descuento, Pago, Tipo, Importe FROM ventas WHERE id_venta >= @id AND year(fecha) = @year AND fecha >= #{fechaInicial}# ORDER BY id_venta ASC";

                    rs = db.Database.SqlQuery<DTO.Venta>(sql,
                        new OleDbParameter("id", (int)id),
                        new OleDbParameter("year", year))
                            .ToList();
                }
            }
            catch (FarmaciaContextException)
            {
                return new List<Venta>();
            }

            var ventas = new List<Venta>();
            foreach (var ventaRegistrada in rs)
            {
                var venta = new Venta
                {
                    Id = ventaRegistrada.Id,
                    Tipo = ventaRegistrada.Tipo.ToString(),
                    FechaHora = ventaRegistrada.Fecha,
                    Puesto = ventaRegistrada.Puesto.ToString(),
                    ClienteId = ventaRegistrada.Cliente,
                    VendedorId = ventaRegistrada.Vendedor,
                    TotalDescuento = ventaRegistrada.Descuento * _factorCentecimal,
                    TotalBruto = ventaRegistrada.Pago * _factorCentecimal,
                    Importe = ventaRegistrada.Importe * _factorCentecimal,
                };

                if (ventaRegistrada.Cliente > 0)
                    venta.Cliente = _clientesRepository.GetOneOrDefaultById(ventaRegistrada.Cliente, false); // TODO verficar hacer

                var ticket = _ticketRepository.GetOneOrdefaultByVentaId(ventaRegistrada.Id, fecha.Year);
                if (ticket != null)
                {
                    venta.Ticket = new Ticket
                    {
                        Numero = ticket.Numero,
                        Serie = ticket.Serie
                    };
                }

                venta.VendedorNombre = _vendedoresRepository.GetOneOrDefaultById(ventaRegistrada.Vendedor)?.Nombre;
                venta.Detalle = GetDetalleDeVentaByVentaId(fecha.Year, ventaRegistrada.Id);

                ventas.Add(venta);
            }

            return ventas;
        }

        public List<VentaDetalle> GetDetalleDeVentaByVentaId(long venta)
        {
            string connectionString = @"User Id=""CONSU""; Password=""consu"";" +
                @"Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=IPC)(KEY=DP9))" +
                    "(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.0.30)(PORT=1521)))(CONNECT_DATA=(INSTANCE_NAME=DP9)(SERVICE_NAME=ORACLE9)))";

            var conn = new OracleConnection(connectionString);

            try
            {
                conn.Open();
                var sql = $@"SELECT
                                VTA_OPERACION, LINEA_VENTA,
                                ENT_CODIGO, ENTTP_TIPO,
                                PVP_ORIGINAL_E, PVP_APORTACION_E, IMP_DTO_E,
                                ART_CODIGO, DESCRIPCION, UNIDADES
                                FROM appul.ah_venta_lineas
                                WHERE emp_codigo = 'EMP1' AND situacion = 'N' AND vta_operacion='{venta}'";

                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                var detalle = new List<VentaDetalle>();
                while (!reader.Read())
                {
                    var vtaOperacion = Convert.ToInt64(reader["VTA_OPERACION"]);
                    var lineaVenta = Convert.ToInt32(reader["LINEA_VENTA"]);
                    var entCodigo = !Convert.IsDBNull(reader["ENT_CODIGO"]) ? (int?)Convert.ToInt32(reader["ENT_CODIGO"]) : null;
                    var enttpTipo = !Convert.IsDBNull(reader["ENTTP_TIPO"]) ? (int?)Convert.ToInt32(reader["ENTTP_TIPO"]) : null; ;
                    var pvpOriginalE = !Convert.IsDBNull(reader["PVP_ORIGINAL_E"]) ? Convert.ToDecimal(reader["PVP_ORIGINAL_E"]) : 0;
                    var pvpAportacionE = !Convert.IsDBNull(reader["PVP_APORTACION_E"]) ? Convert.ToDecimal(reader["PVP_APORTACION_E"]) : 0;
                    var impDtoE = !Convert.IsDBNull(reader["IMP_DTO_E"]) ? Convert.ToDecimal(reader["IMP_DTO_E"]) : 0;
                    var artCodigo = Convert.ToString(reader["ART_CODIGO"]);
                    var unidades = Convert.ToInt32(reader["UNIDADES"]);

                    var descripcion = reader["DESCRIPCION"];

                    var ventaDetalle = new VentaDetalle
                    {
                        VentaId = vtaOperacion,
                        Linea = lineaVenta,
                        Receta = !entCodigo.HasValue ? string.Empty
                            : !enttpTipo.HasValue ? entCodigo.Value.ToString()
                            : $"{entCodigo.Value} {enttpTipo.Value}",
                        PVP = pvpOriginalE,
                        Precio = pvpAportacionE,
                        Descuento = impDtoE,
                        Cantidad = unidades
                        //Importe = item.Importe * _factorCentecimal,
                    };

                    var farmaco = _farmacoRepository.GetOneOrDefaultById(artCodigo);
                    if (farmaco != null)
                    {
                        var proveedor = _proveedorRepository.GetOneOrDefaultByCodigoNacional(artCodigo);
                        var categoria = _categoriaRepository.GetOneOrDefaultById(artCodigo);

                        Familia familia = null;
                        Familia superFamilia = null;
                        if (string.IsNullOrWhiteSpace(farmaco.SubFamilia))
                        {
                            familia = new Familia { Nombre = string.Empty };
                            superFamilia = _familiaRepository.GetOneOrDefaultById(farmaco.Familia)
                                ?? new Familia { Nombre = string.Empty };
                        }
                        else
                        {
                            familia = _familiaRepository.GetSubFamiliaOneOrDefault(farmaco.Familia, farmaco.SubFamilia)
                                ?? new Familia { Nombre = string.Empty };
                            superFamilia = _familiaRepository.GetOneOrDefaultById(farmaco.Familia)
                                ?? new Familia { Nombre = string.Empty };
                        }

                        var laboratorio = !farmaco.Laboratorio.HasValue ? new Laboratorio { Codigo = string.Empty, Nombre = "<Sin Laboratorio>" }
                            : _laboratorioRepository.GetOneOrDefaultByCodigo(farmaco.Laboratorio.Value, farmaco.Clase, farmaco.ClaseBot)
                                ?? new Laboratorio { Codigo = string.Empty, Nombre = "<Sin Laboratorio>" };

                        ventaDetalle.Farmaco = new Farmaco
                        {
                            Codigo = artCodigo,
                            PrecioCoste = farmaco.PUC,
                            CodigoBarras = farmaco.CodigoBarras,
                            Proveedor = proveedor,
                            Categoria = categoria,
                            Familia = familia,
                            SuperFamilia = superFamilia,
                            Laboratorio = laboratorio,
                            Denominacion = farmaco.Denominacion
                        };
                    }
                    detalle.Add(ventaDetalle);
                }

                return detalle;
            }
            catch (Exception ex)
            {
                return new List<VentaDetalle>();
            }
            finally
            {
                conn.Close();
            }
        }

        public List<VentaDetalle> GetDetalleDeVentaByVentaId(int year, long venta)
        {
            var ventaInteger = (int)venta;

            try
            {
                using (var db = FarmaciaContext.VentasByYear(year))
                {
                    var sql = @"SELECT ID_Farmaco as Farmaco, Organismo, Cantidad, PVP, DescLin as Descuento, Importe FROM lineas_venta WHERE ID_venta= @venta";
                    var lineas = db.Database.SqlQuery<DTO.LineaVenta>(sql,
                        new OleDbParameter("venta", ventaInteger))
                        .ToList();

                    var linea = 0;
                    var detalle = new List<VentaDetalle>();
                    foreach (var item in lineas)
                    {
                        var ventaDetalle = new VentaDetalle
                        {
                            Linea = ++linea,
                            Importe = item.Importe * _factorCentecimal,
                            PVP = item.PVP * _factorCentecimal,
                            Descuento = item.Descuento * _factorCentecimal,
                            Receta = item.Organismo,
                            Cantidad = item.Cantidad
                        };

                        var farmaco = _farmacoRepository.GetOneOrDefaultById(item.Farmaco.ToString());
                        if (farmaco != null)
                        {
                            var pcoste = farmaco.PrecioUnicoEntrada.HasValue && farmaco.PrecioUnicoEntrada != 0
                                ? (decimal)farmaco.PrecioUnicoEntrada.Value * _factorCentecimal
                                : ((decimal?)farmaco.PrecioMedio ?? 0m) * _factorCentecimal;

                            var codigoBarra = _barraRepository.GetOneByFarmacoId(farmaco.Id);
                            var proveedor = _proveedorRepository.GetOneOrDefaultByCodigoNacional(farmaco.Id.ToString());

                            var categoria = farmaco.CategoriaId.HasValue
                                ? _categoriaRepository.GetOneOrDefaultById(farmaco.CategoriaId.Value.ToString())
                                : null;

                            var subcategoria = farmaco.CategoriaId.HasValue && farmaco.SubcategoriaId.HasValue
                                ? _categoriaRepository.GetSubcategoriaOneOrDefaultByKey(
                                    farmaco.CategoriaId.Value,
                                    farmaco.SubcategoriaId.Value)
                                : null;

                            var familia = _familiaRepository.GetOneOrDefaultById(farmaco.Familia);
                            var laboratorio = _laboratorioRepository.GetOneOrDefaultByCodigo(farmaco.Laboratorio.Value, null, null) // TODO check clase y clasebot
                                ?? new Laboratorio { Codigo = farmaco.Laboratorio.Value.ToString() };

                            ventaDetalle.Farmaco = new Farmaco
                            {
                                Id = farmaco.Id,
                                Codigo = item.Farmaco.ToString(),
                                PrecioCoste = pcoste,
                                CodigoBarras = codigoBarra,
                                Proveedor = proveedor,
                                Categoria = categoria,
                                Subcategoria = subcategoria,
                                Familia = familia,
                                Laboratorio = laboratorio,
                                Denominacion = farmaco.Denominacion
                            };
                        }
                        else ventaDetalle.Farmaco = new Farmaco { Id = item.Farmaco, Codigo = item.Farmaco.ToString() };

                        detalle.Add(ventaDetalle);
                    }

                    return detalle;
                }
            }
            catch (FarmaciaContextException)
            {
                return new List<VentaDetalle>();
            }
        }

        public Ticket GetOneOrDefaultTicketByVentaId(long id)
        {
            var year = int.Parse($"{id}".Substring(0, 4));
            var ventaId = int.Parse($"{id}".Substring(4));

            using (var db = FarmaciaContext.VentasByYear(year))
            {
                var sql = @"SELECT Id_Ticket as Numero, Serie FROM Tickets_D WHERE Id_Venta = @venta";
                var rs = db.Database.SqlQuery<DTO.Ticket>(sql,
                    new OleDbParameter("venta", ventaId))
                    .FirstOrDefault();

                return rs != null ? new Ticket { Numero = rs.Numero, Serie = rs.Serie } : null;
            }
        }
    }
}