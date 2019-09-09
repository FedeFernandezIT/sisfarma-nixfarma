using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public class EncargosRepository : FarmaciaRepository, IEncargosRepository
    {
        private readonly IClientesRepository _clientesRepository;
        private readonly IProveedorRepository _proveedorRepository;
        private readonly IFarmacoRepository _farmacoRepository;
        private readonly ICategoriaRepository _categoriaRepository;
        private readonly IFamiliaRepository _familiaRepository;
        private readonly ILaboratorioRepository _laboratorioRepository;
        private readonly IVendedoresRepository _vendedoresRepository;

        private readonly decimal _factorCentecimal = 0.01m;

        public EncargosRepository(LocalConfig config) : base(config)
        { }

        public EncargosRepository(
            IClientesRepository clientesRepository,
            IProveedorRepository proveedorRepository,
            IFarmacoRepository farmacoRepository,
            ICategoriaRepository categoriaRepository,
            IFamiliaRepository familiaRepository,
            ILaboratorioRepository laboratorioRepository,
            IVendedoresRepository vendedoresRepository)
        {
            _clientesRepository = clientesRepository ?? throw new ArgumentNullException(nameof(clientesRepository));
            _proveedorRepository = proveedorRepository ?? throw new ArgumentNullException(nameof(proveedorRepository));
            _farmacoRepository = farmacoRepository ?? throw new ArgumentNullException(nameof(farmacoRepository));
            _categoriaRepository = categoriaRepository ?? throw new ArgumentNullException(nameof(categoriaRepository));
            _familiaRepository = familiaRepository ?? throw new ArgumentNullException(nameof(familiaRepository));
            _laboratorioRepository = laboratorioRepository ?? throw new ArgumentNullException(nameof(laboratorioRepository));
            _vendedoresRepository = vendedoresRepository ?? throw new ArgumentNullException(nameof(vendedoresRepository));
        }

        public IEnumerable<Encargo> GetAllByContadorGreaterOrEqual(int year, long? contador)
        {
            using (var db = FarmaciaContext.Create(_config))
            {
                var sql = @"SELECT TOP 1000 * From Encargo WHERE year(idFecha) >= @year AND IdContador >= @contador Order by IdContador ASC";
                return db.Database.SqlQuery<Encargo>(sql,
                    new SqlParameter("year", year),
                    new SqlParameter("contador", contador ?? SqlInt64.Null))
                    .ToList();
            }
        }

        public IEnumerable<Encargo> GetAllByIdGreaterOrEqual(int year, long encargo)
        {
            var conn = FarmaciaContext.GetConnection();
            try
            {
                var rs = new List<DTO.Encargo>();
                var sqlExtra = string.Empty;
                var sql = $@"
                    SELECT e.num_enc, e.fecha_enc, e.cliente, e.operador, e.observaciones, e.emp_codigo, e.alm_codigo
                    From appul.ah_encargos e
                    WHERE rownum <= 10 AND to_char(e.fecha_enc, 'YYYY') >= {year} AND e.num_enc >= {encargo} Order by e.num_enc ASC";

                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var rNumEnc = Convert.ToInt32(reader["num_enc"]);
                    var rFechaEnc = Convert.ToDateTime(reader["fecha_enc"]);
                    var rOperador = Convert.ToString(reader["operador"]);
                    var rCliente = !Convert.IsDBNull(reader["cliente"]) ? Convert.ToInt64(reader["cliente"]) : 0L;
                    var rObservaciones = Convert.ToString(reader["observaciones"]);
                    var rEmpCodigo = Convert.ToString(reader["emp_codigo"]);
                    var rAlmCodigo = Convert.ToInt32(reader["alm_codigo"]);

                    sql = $@"
                    SELECT le.linea, le.articulo, le.uni_encargadas, le.fecha_disponib, le.emp_codigo
                    From appul.ah_lin_encargo le
                    where num_enc = {rNumEnc} AND emp_codigo = '{rEmpCodigo}'
                        AND alm_codigo = '{rAlmCodigo}' AND to_char(fecha_enc, 'YYYYMMDD') = '{rFechaEnc.ToString("yyyyMMdd")}'";

                    cmd.CommandText = sql;
                    var readerLineaEncargo = cmd.ExecuteReader();
                    var numLinea = 0;
                    while (readerLineaEncargo.Read())
                    {
                        var rLinea = Convert.ToInt32(readerLineaEncargo["linea"]);
                        var rArticulo = Convert.ToString(readerLineaEncargo["articulo"]);
                        var rUniEncargadas = !Convert.IsDBNull(readerLineaEncargo["uni_encargadas"]) ? Convert.ToInt64(readerLineaEncargo["uni_encargadas"]) : 0L;
                        var rFechaDisponib = !Convert.IsDBNull(readerLineaEncargo["fecha_disponib"]) ? Convert.ToDateTime(readerLineaEncargo["fecha_disponib"]) : DateTime.MinValue;

                        var dto = new DTO.Encargo
                        {
                            Id = rNumEnc,
                            FechaHora = rFechaEnc,
                            Cliente = rCliente,
                            Vendedor = rOperador,
                            Observaciones = rObservaciones,
                            Empresa = rEmpCodigo,
                            Almacen = rAlmCodigo,
                            Linea = ++numLinea,
                            Farmaco = rArticulo,
                            Cantidad = rUniEncargadas,
                            FechaHoraEntrega = rFechaDisponib
                        };

                        rs.Add(dto);
                    }
                }

                return rs.Select(GenerarEncargo);
            }
            catch (Exception ex)
            {
                return new List<Encargo>();
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        private Encargo GenerarEncargo(DTO.Encargo encargo)
        {
            var cliente = new Cliente { Id = encargo.Cliente };
            var vendedor = new Vendedor { Nombre = encargo.Vendedor.Trim() };

            var farmacoEncargado = default(Farmaco);
            var farmaco = _farmacoRepository.GetOneOrDefaultById(encargo.Farmaco);
            if (farmaco != null)
            {
                var proveedor = _proveedorRepository.GetOneOrDefaultByCodigoNacional(encargo.Farmaco);
                var categoria = _categoriaRepository.GetOneOrDefaultById(encargo.Farmaco);

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

                farmacoEncargado = new Farmaco
                {
                    Id = farmaco.Id,
                    Codigo = encargo.Farmaco.ToString(),
                    PrecioCoste = farmaco.PUC,
                    Proveedor = proveedor,
                    Categoria = categoria,
                    Familia = familia,
                    SuperFamilia = superFamilia,
                    Laboratorio = laboratorio,
                    Denominacion = farmaco.Denominacion,
                    Precio = farmaco.PrecioMedio,
                };
            }

            return new Encargo
            {
                Id = encargo.Id,
                Fecha = encargo.FechaHora,
                FechaEntrega = encargo.FechaHoraEntrega,
                Farmaco = farmacoEncargado,
                Cantidad = encargo.Cantidad,
                Cliente = cliente,
                Vendedor = vendedor,
                Observaciones = encargo.Observaciones,
                Empresa = encargo.Empresa,
                Almacen = encargo.Almacen,
                Linea = encargo.Linea
            };
        }

        public IEnumerable<Encargo> GetAllGreaterOrEqualByFecha(DateTime fecha)
        {
            using (var db = FarmaciaContext.Create(_config))
            {
                var sql = @"SELECT * From Encargo WHERE idFecha >= @fecha AND estado > 0 Order by idFecha DESC";
                return db.Database.SqlQuery<Encargo>(sql,
                    new SqlParameter("fecha", fecha))
                    .ToList();
            }
        }
    }
}