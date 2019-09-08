using Oracle.DataAccess.Client;
using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Linq;

using DC = Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public interface IFarmacoRepository
    {
        DTO.Farmaco GetOneOrDefaultById(string id);
    }

    public class FarmacoRespository : FarmaciaRepository, IFarmacoRepository, DC.IFarmacosRepository
    {
        private readonly ICategoriaRepository _categoriaRepository;
        private readonly ICodigoBarraRepository _barraRepository;
        private readonly DC.IFamiliaRepository _familiaRepository;
        private readonly DC.ILaboratorioRepository _laboratorioRepository;
        private readonly DC.IProveedorRepository _proveedorRepository;
        private readonly ITarifaRepository _tarifaRepository;

        private readonly decimal _factorCentecimal = 0.01m;

        public FarmacoRespository(LocalConfig config)
            : base(config)
        { }

        public FarmacoRespository()
        {
        }

        public FarmacoRespository(
            ICategoriaRepository categoriaRepository,
            ICodigoBarraRepository barraRepository,
            DC.IFamiliaRepository familiaRepository,
            DC.ILaboratorioRepository laboratorioRepository,
            DC.IProveedorRepository proveedorRepository,
            ITarifaRepository tarifaRepository)
        {
            _categoriaRepository = categoriaRepository ?? throw new ArgumentNullException(nameof(categoriaRepository));
            _barraRepository = barraRepository ?? throw new ArgumentNullException(nameof(barraRepository));
            _familiaRepository = familiaRepository ?? throw new ArgumentNullException(nameof(familiaRepository));
            _laboratorioRepository = laboratorioRepository ?? throw new ArgumentNullException(nameof(laboratorioRepository));
            _proveedorRepository = proveedorRepository ?? throw new ArgumentNullException(nameof(proveedorRepository));
            _tarifaRepository = tarifaRepository ?? throw new ArgumentNullException(nameof(tarifaRepository));
        }

        public DTO.Farmaco GetOneOrDefaultById(string id)
        {
            string connectionString = @"User Id=""CONSU""; Password=""consu"";" +
                @"Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=IPC)(KEY=DP9))" +
                    "(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.0.30)(PORT=1521)))(CONNECT_DATA=(INSTANCE_NAME=DP9)(SERVICE_NAME=ORACLE9)))";

            var conn = new OracleConnection(connectionString);

            try
            {
                conn.Open();
                var sql = $@"SELECT
                    EAN_13, PRECIO_LAB_EUROS,
                    FAMSB_CODIGO, FAM_CODIGO,
                    LAB_CODIGO, CLASE, CLASE_BOT,
                    DESCRIPCION
                    FROM appul.ab_articulos where codigo = '{id}'";

                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                if (!reader.Read())
                    return null;

                var ean13 = Convert.ToString(reader["EAN_13"]);
                var precioLabEuros = !Convert.IsDBNull(reader["PRECIO_LAB_EUROS"]) ? Convert.ToDecimal(reader["PRECIO_LAB_EUROS"]) : 0;
                var fambsCodigo = Convert.ToString(reader["FAMSB_CODIGO"]);
                var famCodigo = Convert.ToInt32(reader["FAM_CODIGO"]);
                var labCodigo = !Convert.IsDBNull(reader["LAB_CODIGO"]) ? (long?)Convert.ToInt64(reader["LAB_CODIGO"]) : null;
                var clase = Convert.ToString(reader["CLASE"]);
                var claseBot = Convert.ToString(reader["CLASE_BOT"]);
                var descripcion = Convert.ToString(reader["DESCRIPCION"]);

                var farmaco = new DTO.Farmaco
                {
                    CodigoBarras = !string.IsNullOrWhiteSpace(ean13) ? ean13
                        : "847000" + id.ToString().PadLeft(6, '0'),
                    Familia = famCodigo,
                    SubFamilia = fambsCodigo,
                    Laboratorio = labCodigo,
                    Clase = clase,
                    ClaseBot = claseBot,
                    Denominacion = descripcion
                };

                // cargar precios
                sql = $@"SELECT
                        NVL(MAX(ubicacion),'') AS UBICACION,
                        NVL(MAX(pmc_es),0) AS PCMEDIO,
                        NVL(MAX(puc_es),0) AS PUC
                        FROM appul.ac_existencias WHERE art_codigo = '{id}'";

                cmd.CommandText = sql;
                reader = cmd.ExecuteReader();

                var puc = 0m;
                var pcm = 0m;
                var ubicacion = string.Empty;
                if (reader.Read())
                {
                    puc = Convert.ToDecimal(reader["PUC"]);
                    pcm = Convert.ToDecimal(reader["PCMEDIO"]);
                    ubicacion = Convert.ToString(reader["UBICACION"]);
                }

                farmaco.PUC = puc != 0 ? puc
                    : pcm != 0 ? pcm
                    : precioLabEuros;

                farmaco.Ubicacion = ubicacion;

                return farmaco;
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                conn.Close();
            }
        }

        public IEnumerable<Farmaco> GetAllByFechaUltimaEntradaGreaterOrEqual(DateTime fecha)
        {
            var rs = Enumerable.Empty<DTO.Farmaco>();
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE Fecha_U_Entrada >= @fecha ORDER BY Fecha_U_Entrada ASC";
                rs = db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("fecha", fecha.ToDateInteger("yyyyMMdd")))
                    .ToList();
            }

            return rs.Select(GenerarFarmaco);
        }

        public IEnumerable<DTO.Farmaco> GetAllByFechaUltimaEntradaGreaterOrEqualAsDTO(DateTime fecha)
        {
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE Fecha_U_Entrada >= @fecha ORDER BY Fecha_U_Entrada ASC";
                return db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("fecha", fecha.ToDateInteger("yyyyMMdd")))
                    .ToList();
            }
        }

        public IEnumerable<Farmaco> GetAllByFechaUltimaSalidaGreaterOrEqual(DateTime fecha)
        {
            var rs = Enumerable.Empty<DTO.Farmaco>();
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE Fecha_U_Salida >= @fecha ORDER BY Fecha_U_Salida ASC";
                rs = db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("fecha", fecha.ToDateInteger("yyyyMMdd")))
                    .ToList();
            }

            return rs.Select(GenerarFarmaco);
        }

        public IEnumerable<DTO.Farmaco> GetAllByFechaUltimaSalidaGreaterOrEqualAsDTO(DateTime fecha)
        {
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE Fecha_U_Salida >= @fecha ORDER BY Fecha_U_Salida ASC";
                return db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("fecha", fecha.ToDateInteger("yyyyMMdd")))
                    .ToList();
            }
        }

        public IEnumerable<Farmaco> GetAllWithoutStockByIdGreaterOrEqual(string codigo)
        {
            var rs = Enumerable.Empty<DTO.Farmaco>();
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE ID_Farmaco >= @codigo AND (existencias <= 0 OR existencias IS NULL) ORDER BY ID_Farmaco ASC";
                rs = db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("codigo", int.Parse(codigo)))
                    .ToList();
            }

            return rs.Select(GenerarFarmaco);
        }

        public IEnumerable<DTO.Farmaco> GetAllWithoutStockByIdGreaterOrEqualAsDTO(string codigo)
        {
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE ID_Farmaco >= @codigo AND (existencias <= 0 OR existencias IS NULL) ORDER BY ID_Farmaco ASC";
                return db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("codigo", int.Parse(codigo)))
                    .ToList();
            }
        }

        public IEnumerable<Farmaco> GetWithStockByIdGreaterOrEqual(string codigo)
        {
            var rs = Enumerable.Empty<DTO.Farmaco>();
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE ID_Farmaco >= @codigo AND existencias > 0 ORDER BY ID_Farmaco ASC";
                rs = db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("codigo", int.Parse(codigo)))
                    .ToList();
            }

            return rs.Select(GenerarFarmaco);
        }

        public IEnumerable<DTO.Farmaco> GetWithStockByIdGreaterOrEqualAsDTO(string codArticu)
        {
            var farmacos = new List<DTO.Farmaco>();
            var conn = FarmaciaContext.GetConnection();
            try
            {
                var sqlExtra = string.Empty;
                var sql = $@"
                    select a.codigo, a.precio_lab_euros, a.Pvp_euros, a.famsb_codigo, a.fam_codigo, a.descripcion, a.lab_codigo, a.clase, a.clase_bot,
                           a.imp_codigo, a.ean_13, a.Fecha_Baja, sum(e.actuales) as stock, max(e.stock_min) as stock_minimo, max(e.stock_max) as stock_maximo,
                           max(to_date(e.fecha_caducidad)) as fecha_caducidad, max(e.fuc) AS fuc, max(e.fuv) as fuv,
                           NVL(MAX(e.pmc_es),0) AS pcmedio, NVL(MAX(e.puc_es),0) AS puc, NVL(MAX(e.ubicacion),'') AS ubicacion
                    from (select distinct
                                codigo, precio_lab_euros, Pvp_euros, famsb_codigo, fam_codigo, descripcion, lab_codigo, clase, clase_bot,
                                imp_codigo, ean_13, Fecha_Baja from appul.ab_articulos {sqlExtra}) a
                    LEFT JOIN appul.ac_existencias e ON e.art_codigo = a.codigo
                    WHERE a.codigo >= '{codArticu.PadLeft(6, '0')}'
                    GROUP BY a.codigo, a.precio_lab_euros, a.Pvp_euros, a.famsb_codigo, a.fam_codigo,
                            a.descripcion, a.lab_codigo, a.clase, a.clase_bot, a.imp_codigo, a.ean_13, a.Fecha_Baja
                    HAVING NVL(sum(e.actuales),0) > 0 ORDER BY a.codigo ASC";

                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var rCodigo = Convert.ToString(reader["codigo"]);
                    var rPrecioLabEuros = !Convert.IsDBNull(reader["precio_lab_euros"]) ? (decimal?)Convert.ToDecimal(reader["precio_lab_euros"]) : null;
                    var rPvpEuros = !Convert.IsDBNull(reader["Pvp_euros"]) ? (decimal?)Convert.ToDecimal(reader["Pvp_euros"]) : null;
                    var rFamsbCodigo = Convert.ToString(reader["famsb_codigo"]);
                    var rFamCodigo = Convert.ToString(reader["fam_codigo"]);
                    var rDescripcion = Convert.ToString(reader["descripcion"]);
                    var rLabCodigo = !Convert.IsDBNull(reader["lab_codigo"]) ? (long?)Convert.ToInt64(reader["lab_codigo"]) : null;
                    var rClase = Convert.ToString(reader["clase"]);
                    var rClaseBot = Convert.ToString(reader["clase_bot"]);
                    var rImpCodigo = Convert.ToString(reader["imp_codigo"]);
                    var rEan13 = Convert.ToString(reader["ean_13"]);
                    var rFechaBaja = !Convert.IsDBNull(reader["Fecha_Baja"]) ? (DateTime?)Convert.ToDateTime(reader["Fecha_Baja"]) : null;
                    var rStock = !Convert.IsDBNull(reader["stock"]) ? Convert.ToInt64(reader["stock"]) : 0L;
                    var rStockMinimo = Convert.ToInt64(reader["stock_minimo"]);
                    var rStockMaximo = Convert.ToInt64(reader["stock_maximo"]);
                    var rFechaCaducidad = !Convert.IsDBNull(reader["fecha_caducidad"]) ? (DateTime?)Convert.ToDateTime(reader["fecha_caducidad"]) : null;
                    var rFuc = !Convert.IsDBNull(reader["fuc"]) ? (DateTime?)Convert.ToDateTime(reader["fuc"]) : null;
                    var rFuv = !Convert.IsDBNull(reader["fuv"]) ? (DateTime?)Convert.ToDateTime(reader["fuv"]) : null;
                    var rPcMedio = !Convert.IsDBNull(reader["pcmedio"]) ? Convert.ToDecimal(reader["pcmedio"]) : 0m;
                    var rPuc = !Convert.IsDBNull(reader["puc"]) ? Convert.ToDecimal(reader["puc"]) : 0m;
                    var rUbicacion = Convert.ToString(reader["ubicacion"]);

                    var farmaco = new DTO.Farmaco
                    {
                        Codigo = rCodigo,
                        Stock = rStock,
                        StockMinimo = rStockMinimo,
                        StockMaximo = rStockMaximo,
                        FechaCaducidad = rFechaCaducidad,
                        FechaUltimaCompra = rFuc,
                        FechaUltimaVenta = rFuv,
                        PrecioCoste = rPuc != 0m ? rPuc
                            : rPcMedio != 0m ? rPcMedio
                            : rPrecioLabEuros ?? 0m,
                        Precio = rPvpEuros ?? 0m,
                        Familia = rFamCodigo.ToIntegerOrDefault(),
                        SubFamilia = rFamsbCodigo,
                        Laboratorio = rLabCodigo,
                        CodigoBarras = rEan13,
                        CodigoImpuesto = rImpCodigo,
                        Denominacion = rDescripcion,
                        FechaBaja = rFechaBaja
                    };

                    farmacos.Add(farmaco);
                }

                return farmacos;
            }
            catch (Exception ex)
            {
                return farmacos;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        public bool AnyGraterThatDoesnHaveStock(string codigo)
        {
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 1 ID_Farmaco as Id FROM Farmacos WHERE ID_Farmaco > @codigo AND existencias <= 0 ORDER BY ID_Farmaco ASC";
                var rs = db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("codigo", int.Parse(codigo)))
                    .FirstOrDefault();

                return rs != null;
            }
        }

        public bool AnyGreaterThatHasStock(string codigo)
        {
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 1 ID_Farmaco as Id FROM Farmacos WHERE ID_Farmaco > @codigo AND existencias > 0 ORDER BY ID_Farmaco ASC";
                var rs = db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("codigo", int.Parse(codigo)))
                    .FirstOrDefault();

                return rs != null;
            }
        }

        public Farmaco GenerarFarmaco(DTO.Farmaco farmaco)
        {
            var proveedor = _proveedorRepository.GetOneOrDefaultByCodigoNacional(farmaco.Codigo);
            var categoria = _categoriaRepository.GetOneOrDefaultById(farmaco.Codigo);

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

            var impuesto = !string.IsNullOrWhiteSpace(farmaco.CodigoImpuesto) ? farmaco.CodigoImpuesto : "0";
            var iva = _tarifaRepository.GetTarifaOrDefaultByCodigoImpuesto(impuesto) ?? 0m;

            return new Farmaco
            {
                Id = farmaco.Id,
                Codigo = farmaco.Codigo,
                Denominacion = farmaco.Denominacion,
                Familia = familia,
                Categoria = categoria,
                CodigoBarras = farmaco.CodigoBarras,
                Proveedor = proveedor,
                FechaUltimaCompra = farmaco.FechaUltimaCompra.HasValue ? (DateTime?)$"{farmaco.FechaUltimaCompra.Value}".ToDateTimeOrDefault("yyyyMMdd") : null,
                FechaUltimaVenta = farmaco.FechaUltimaVenta.HasValue ? (DateTime?)$"{farmaco.FechaUltimaVenta.Value}".ToDateTimeOrDefault("yyyyMMdd") : null,
                FechaCaducidad = farmaco.FechaCaducidad.HasValue ? (DateTime?)$"{farmaco.FechaCaducidad.Value}".ToDateTimeOrDefault("yyyyMM") : null,
                Ubicacion = farmaco.Ubicacion ?? string.Empty,
                Precio = farmaco.Precio,
                PrecioCoste = farmaco.PrecioCoste,
                Iva = iva,
                Stock = farmaco.Stock,
                StockMinimo = farmaco.StockMinimo,
                StockMaximo = farmaco.StockMaximo,
                Laboratorio = laboratorio,
                Baja = farmaco.FechaBaja.HasValue,
            };
        }

        public bool Exists(string codigo) => GetOneOrDefaultById(codigo) != null;
    }
}