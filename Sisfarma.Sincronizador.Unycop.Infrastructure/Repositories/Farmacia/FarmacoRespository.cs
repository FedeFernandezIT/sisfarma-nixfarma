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
            DC.IProveedorRepository proveedorRepository)
        {
            _categoriaRepository = categoriaRepository ?? throw new ArgumentNullException(nameof(categoriaRepository));
            _barraRepository = barraRepository ?? throw new ArgumentNullException(nameof(barraRepository));
            _familiaRepository = familiaRepository ?? throw new ArgumentNullException(nameof(familiaRepository));
            _laboratorioRepository = laboratorioRepository ?? throw new ArgumentNullException(nameof(laboratorioRepository));
            _proveedorRepository = proveedorRepository ?? throw new ArgumentNullException(nameof(proveedorRepository));
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

        public IEnumerable<DTO.Farmaco> GetWithStockByIdGreaterOrEqualAsDTO(string codigo)
        {
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE ID_Farmaco >= @codigo AND existencias > 0 ORDER BY ID_Farmaco ASC";
                return db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("codigo", int.Parse(codigo)))
                    .ToList();
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
            var familia = _familiaRepository.GetOneOrDefaultById(farmaco.Familia);
            var categoria = farmaco.CategoriaId.HasValue
                            ? _categoriaRepository.GetOneOrDefaultById(farmaco.CategoriaId.Value.ToString())
                            : null;

            var subcategoria = farmaco.CategoriaId.HasValue && farmaco.SubcategoriaId.HasValue
                ? _categoriaRepository.GetSubcategoriaOneOrDefaultByKey(
                    farmaco.CategoriaId.Value,
                    farmaco.SubcategoriaId.Value)
                : null;

            var codigoBarra = _barraRepository.GetOneByFarmacoId(farmaco.Id);

            var proveedor = _proveedorRepository.GetOneOrDefaultByCodigoNacional(farmaco.Id.ToString());

            var laboratorio = _laboratorioRepository.GetOneOrDefaultByCodigo(farmaco.Laboratorio.Value, null, null); // TODO check clase clasebot

            var pcoste = farmaco.PrecioUnicoEntrada.HasValue && farmaco.PrecioUnicoEntrada != 0
                            ? (decimal)farmaco.PrecioUnicoEntrada.Value * _factorCentecimal
                            : ((decimal?)farmaco.PrecioMedio ?? 0m) * _factorCentecimal;

            var iva = default(decimal);
            switch (farmaco.IVA)
            {
                case 1: iva = 4; break;

                case 2: iva = 10; break;

                case 3: iva = 21; break;

                default: iva = 0; break;
            }

            return new Farmaco
            {
                Id = farmaco.Id,
                Codigo = farmaco.Id.ToString(),
                Denominacion = farmaco.Denominacion,
                Familia = familia,
                Categoria = categoria,
                Subcategoria = subcategoria,
                CodigoBarras = codigoBarra,
                Proveedor = proveedor,
                FechaUltimaCompra = farmaco.FechaUltimaEntrada.HasValue ? (DateTime?)$"{farmaco.FechaUltimaEntrada.Value}".ToDateTimeOrDefault("yyyyMMdd") : null,
                FechaUltimaVenta = farmaco.FechaUltimaSalida.HasValue ? (DateTime?)$"{farmaco.FechaUltimaSalida.Value}".ToDateTimeOrDefault("yyyyMMdd") : null,
                Ubicacion = farmaco.Ubicacion ?? string.Empty,
                Web = farmaco.BolsaPlastico,
                Precio = farmaco.PVP * _factorCentecimal,
                PrecioCoste = pcoste,
                Iva = iva,
                Stock = farmaco.Existencias ?? 0,
                StockMinimo = farmaco.Stock ?? 0,
                Laboratorio = laboratorio,
                Baja = farmaco.FechaBaja > 0,
                FechaCaducidad = farmaco.FechaCaducidad.HasValue ? (DateTime?)$"{farmaco.FechaCaducidad.Value}".ToDateTimeOrDefault("yyyyMM") : null
            };
        }

        public bool Exists(string codigo) => GetOneOrDefaultById(codigo) != null;
    }
}