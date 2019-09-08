using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia.DTO;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DC = Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using DE = Sisfarma.Sincronizador.Domain.Entities.Farmacia;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public interface IRecepcionRespository
    {
        long? GetCodigoProveedorActualOrDefaultByFarmaco(long farmaco);
    }

    public class RecepcionRespository : FarmaciaRepository, IRecepcionRespository, DC.IRecepcionRepository
    {
        private readonly IProveedorRepository _proveedorRepository;
        private readonly IFarmacoRepository _farmacoRepository;
        private readonly ICategoriaRepository _categoriaRepository;
        private readonly IFamiliaRepository _familiaRepository;
        private readonly ILaboratorioRepository _laboratorioRepository;

        private readonly decimal _factorCentecimal = 0.01m;

        public RecepcionRespository(LocalConfig config) : base(config)
        { }

        public RecepcionRespository()
        {
        }

        public RecepcionRespository(
            IProveedorRepository proveedorRepository,
            IFarmacoRepository farmacoRepository,
            ICategoriaRepository categoriaRepository,
            IFamiliaRepository familiaRepository,
            ILaboratorioRepository laboratorioRepository)
        {
            _proveedorRepository = proveedorRepository ?? throw new ArgumentNullException(nameof(proveedorRepository));
            _farmacoRepository = farmacoRepository ?? throw new ArgumentNullException(nameof(farmacoRepository));
            _categoriaRepository = categoriaRepository ?? throw new ArgumentNullException(nameof(categoriaRepository));
            _familiaRepository = familiaRepository ?? throw new ArgumentNullException(nameof(familiaRepository));
            _laboratorioRepository = laboratorioRepository ?? throw new ArgumentNullException(nameof(laboratorioRepository));
        }

        public long? GetCodigoProveedorActualOrDefaultByFarmaco(long farmaco)
        {
            var farmacoInteger = (int)farmaco;
            using (var db = FarmaciaContext.Recepcion())
            {
                var sql = "SELECT TOP 1 Proveedor FROM Recepcion WHERE ID_Farmaco = @farmaco ORDER BY ID_Fecha DESC";
                return db.Database.SqlQuery<int?>(sql,
                    new OleDbParameter("farmaco", farmaco))
                    .FirstOrDefault();
            }
        }

        public RecepcionTotales GetTotalesByPedidoAsDTO(int anio, long numeroPedido, string empresa)
        {
            var conn = FarmaciaContext.GetConnection();
            try
            {
                var sqlExtra = string.Empty;
                var sql = $@"
                        select NVL(COUNT(pedido),0) AS numLineas, NVL(SUM(cant_servida*pvp_iva_euros),0) AS importePvp,
                               NVL(SUM(cant_servida*pc_iva_euros),0) AS importePuc
                        from appul.ad_rec_linped
                        where pedido = {numeroPedido} AND cant_servida <> 0
                          AND emp_codigo = '{empresa}'
                          AND to_char(fecha_recepcion, 'YYYY') = {anio}";

                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                if (reader.Read())
                {
                    var rNumLineas = Convert.ToInt32(reader["numLineas"]);
                    var rImportePvp = Convert.ToDecimal(reader["importePvp"]);
                    var rImportePuc = Convert.ToDecimal(reader["importePuc"]);

                    return new RecepcionTotales
                    {
                        Lineas = rNumLineas,
                        PVP = rImportePvp,
                        PUC = rImportePuc
                    };
                }

                return new RecepcionTotales();
            }
            catch (Exception ex)
            {
                return new RecepcionTotales();
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        public IEnumerable<DE.Recepcion> GetAllByYear(int year)
        {
            try
            {
                var rs = Enumerable.Empty<DTO.Recepcion>();
                using (var db = FarmaciaContext.RecepcionByYear(year))
                {
                    var sql = $@"
                        SELECT ID_Fecha as Fecha, AlbaranID as Albaran, Proveedor, ID_Farmaco as Farmaco, PVP, PC, PVAlb as PVAlbaran, PCTotal, Recibido, Bonificado, Devuelto FROM Recepcion
                            WHERE AlbaranID IN (SELECT alb.AlbaranID FROM
                                    (SELECT TOP 999 AlbaranID, ID_Fecha FROM Recepcion
                                        WHERE YEAR(ID_Fecha) >= @year AND (recibido <> 0 OR devuelto <> 0 OR bonificado <> 0) AND ID_Fecha IS NOT NULL AND AlbaranID IS NOT NULL
                                        GROUP BY AlbaranID, ID_Fecha
                                        ORDER BY ID_Fecha ASC) AS alb)
                                AND YEAR(ID_Fecha) >= @year AND (recibido <> 0 OR devuelto <> 0 OR bonificado <> 0) AND ID_Fecha IS NOT NULL AND AlbaranID IS NOT NULL
                            ORDER BY ID_Fecha ASC";
                    rs = db.Database.SqlQuery<DTO.Recepcion>(sql,
                        new OleDbParameter("year", year))
                            .ToList();
                }

                var keys = rs.GroupBy(k => new { k.Fecha.Year, k.Albaran.Value })
                        .ToDictionary(
                            k => new RecepcionCompositeKey { Anio = k.Key.Year, Albaran = k.Key.Value },
                            v => v.ToList());
                return GenerarRecepciones(keys);
            }
            catch (FarmaciaContextException)
            {
                return Enumerable.Empty<DE.Recepcion>();
            }
        }

        public IEnumerable<DTO.Recepcion> GetAllByYearAsDTO(int year)
        {
            var recepciones = new List<DTO.Recepcion>();
            var conn = FarmaciaContext.GetConnection();
            try
            {
                var sqlExtra = string.Empty;
                var sql = $@"
                    SELECT FECHA_RECEPCION, EMP_CODIGO, PEDIDO, PROVEEDOR, ART_CODIGO, PVP_IVA_EUROS, PC_IVA_EUROS, LINEA, CANT_SERVIDA
                    From appul.ad_rec_linped
                    WHERE rownum <= 999 AND
                        to_char(fecha_recepcion, 'YYYY') >= {year} AND cant_servida <> 0
                    Order by to_char(fecha_recepcion, 'YYYYMMDDHH24MISS'), pedido, linea ASC";

                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var rFechaRecepcion = !Convert.IsDBNull(reader["FECHA_RECEPCION"]) ? Convert.ToDateTime(reader["FECHA_RECEPCION"]) : DateTime.MinValue;
                    var rEmpCodigo = Convert.ToString(reader["EMP_CODIGO"]);
                    var rPedido = Convert.ToInt64(reader["PEDIDO"]);
                    var rProveedor = !Convert.IsDBNull(reader["PROVEEDOR"]) ? (long?)Convert.ToInt64(reader["PROVEEDOR"]) : null;
                    var rArtCodigo = Convert.ToString(reader["ART_CODIGO"]);
                    var rPvpIvaEuros = !Convert.IsDBNull(reader["PVP_IVA_EUROS"]) ? (decimal?)Convert.ToDecimal(reader["PVP_IVA_EUROS"]) : null;
                    var rPcIvaEuros = !Convert.IsDBNull(reader["PC_IVA_EUROS"]) ? (decimal?)Convert.ToDecimal(reader["PC_IVA_EUROS"]) : null;
                    var rLinea = !Convert.IsDBNull(reader["LINEA"]) ? Convert.ToInt32(reader["LINEA"]) : 0;
                    var rCantServida = !Convert.IsDBNull(reader["CANT_SERVIDA"]) ? Convert.ToInt64(reader["CANT_SERVIDA"]) : 0L;

                    var pedido = new DTO.Recepcion
                    {
                        Fecha = rFechaRecepcion,
                        Empresa = rEmpCodigo,
                        Proveedor = rProveedor,
                        Pedido = rPedido,
                        Linea = rLinea,
                        Recibido = rCantServida,
                        Farmaco = rArtCodigo,
                        ImportePvp = rPvpIvaEuros ?? 0m,
                        ImportePuc = rPcIvaEuros ?? 0m
                    };

                    recepciones.Add(pedido);
                }

                return recepciones;
            }
            catch (Exception ex)
            {
                return recepciones;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        public IEnumerable<DTO.Recepcion> GetAllByDateAsDTO(DateTime fecha)
        {
            var recepciones = new List<DTO.Recepcion>();
            var conn = FarmaciaContext.GetConnection();
            try
            {
                var sqlExtra = string.Empty;
                var sql = $@"
                    SELECT FECHA_RECEPCION, EMP_CODIGO, PEDIDO, PROVEEDOR, ART_CODIGO, PVP_IVA_EUROS, PC_IVA_EUROS, LINEA, CANT_SERVIDA
                    From appul.ad_rec_linped
                    WHERE rownum <= 999 AND
                        to_char(fecha_recepcion, 'YYYYMMDDHH24MISS') >= {fecha.ToString("yyyyMMddHHmmss")} AND cant_servida <> 0
                    Order by to_char(fecha_recepcion, 'YYYYMMDDHH24MISS'), pedido, linea ASC";

                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var rFechaRecepcion = !Convert.IsDBNull(reader["FECHA_RECEPCION"]) ? Convert.ToDateTime(reader["FECHA_RECEPCION"]) : DateTime.MinValue;
                    var rEmpCodigo = Convert.ToString(reader["EMP_CODIGO"]);
                    var rPedido = Convert.ToInt64(reader["PEDIDO"]);
                    var rProveedor = !Convert.IsDBNull(reader["PROVEEDOR"]) ? (long?)Convert.ToInt64(reader["PROVEEDOR"]) : null;
                    var rArtCodigo = Convert.ToString(reader["ART_CODIGO"]);
                    var rPvpIvaEuros = !Convert.IsDBNull(reader["PVP_IVA_EUROS"]) ? (decimal?)Convert.ToDecimal(reader["PVP_IVA_EUROS"]) : null;
                    var rPcIvaEuros = !Convert.IsDBNull(reader["PC_IVA_EUROS"]) ? (decimal?)Convert.ToDecimal(reader["PC_IVA_EUROS"]) : null;
                    var rLinea = !Convert.IsDBNull(reader["LINEA"]) ? Convert.ToInt32(reader["LINEA"]) : 0;
                    var rCantServida = !Convert.IsDBNull(reader["CANT_SERVIDA"]) ? Convert.ToInt64(reader["CANT_SERVIDA"]) : 0L;

                    var pedido = new DTO.Recepcion
                    {
                        Fecha = rFechaRecepcion,
                        Empresa = rEmpCodigo,
                        Proveedor = rProveedor,
                        Pedido = rPedido,
                        Linea = rLinea,
                        Recibido = rCantServida,
                        Farmaco = rArtCodigo,
                        ImportePvp = rPvpIvaEuros ?? 0m,
                        ImportePuc = rPcIvaEuros ?? 0m
                    };

                    recepciones.Add(pedido);
                }

                return recepciones;
            }
            catch (Exception ex)
            {
                return recepciones;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        internal class RecepcionCompositeKey
        {
            internal int Anio { get; set; }

            internal int Albaran { get; set; }
        }

        private IEnumerable<DE.Recepcion> GenerarRecepciones(Dictionary<RecepcionCompositeKey, List<DTO.Recepcion>> groups)
        {
            var recepciones = new List<DE.Recepcion>();
            foreach (var group in groups)
            {
                var linea = 0;
                var fecha = group.Value.Last().Fecha; // a la vuelta preguntamos por > fecha
                var proveedorPedido = group.Value.First().Proveedor.HasValue ? _proveedorRepository.GetOneOrDefaultById(group.Value.First().Proveedor.Value) : null;
                var detalle = new List<RecepcionDetalle>();
                foreach (var item in group.Value)
                {
                    var recepcionDetalle = new RecepcionDetalle()
                    {
                        Linea = ++linea,
                        RecepcionId = int.Parse($"{group.Key.Anio}{group.Key.Albaran}"),
                        Cantidad = item.Recibido - item.Devuelto,
                        CantidadBonificada = item.Bonificado
                    };

                    var farmaco = _farmacoRepository.GetOneOrDefaultById(item.Farmaco.ToString());
                    if (farmaco != null)
                    {
                        var pcoste = 0m;
                        if (item.PVAlbaran > 0)
                            pcoste = item.PVAlbaran * _factorCentecimal;
                        else if (item.PC > 0)
                            pcoste = item.PC * _factorCentecimal;
                        else
                            pcoste = farmaco.PrecioUnicoEntrada.HasValue && farmaco.PrecioUnicoEntrada != 0
                                ? (decimal)farmaco.PrecioUnicoEntrada.Value * _factorCentecimal
                                : ((decimal?)farmaco.PrecioMedio ?? 0m) * _factorCentecimal;

                        var proveedor = _proveedorRepository.GetOneOrDefaultByCodigoNacional(farmaco.Id.ToString())
                                ?? _proveedorRepository.GetOneOrDefaultById(farmaco.Id);

                        var categoria = farmaco.CategoriaId.HasValue
                            ? _categoriaRepository.GetOneOrDefaultById(farmaco.CategoriaId.Value.ToString())
                            : null;

                        var subcategoria = farmaco.CategoriaId.HasValue && farmaco.SubcategoriaId.HasValue
                            ? _categoriaRepository.GetSubcategoriaOneOrDefaultByKey(
                                farmaco.CategoriaId.Value,
                                farmaco.SubcategoriaId.Value)
                            : null;

                        var familia = _familiaRepository.GetOneOrDefaultById(farmaco.Familia);
                        var laboratorio = _laboratorioRepository.GetOneOrDefaultByCodigo(farmaco.Laboratorio.Value, null, null); // TODO check clase clasebot

                        recepcionDetalle.Farmaco = new DE.Farmaco
                        {
                            Id = farmaco.Id,
                            Codigo = item.Farmaco.ToString(),
                            PrecioCoste = pcoste,
                            Proveedor = proveedor,
                            Categoria = categoria,
                            Subcategoria = subcategoria,
                            Familia = familia,
                            Laboratorio = laboratorio,
                            Denominacion = farmaco.Denominacion,
                            Precio = item.PVP * _factorCentecimal,
                            Stock = farmaco.Existencias ?? 0
                        };
                    }
                    detalle.Add(recepcionDetalle);
                }

                recepciones.Add(new DE.Recepcion
                {
                    Id = int.Parse($"{group.Key.Anio}{group.Key.Albaran}"),
                    Fecha = fecha,
                    Lineas = detalle.Count,
                    ImportePVP = group.Value.Sum(x => x.PVP * x.Recibido * _factorCentecimal),
                    ImportePUC = group.Value.Sum(x => x.PCTotal * _factorCentecimal),
                    Proveedor = proveedorPedido
                }.AddRangeDetalle(detalle));
            }
            return recepciones;
        }

        public IEnumerable<DE.ProveedorHistorico> GetAllHistoricosByFecha(DateTime fecha)
        {
            var rs = Enumerable.Empty<DTO.ProveedorHistorico>();
            using (var db = FarmaciaContext.RecepcionByYear(fecha.Year))
            {
                var sql = $@"SELECT ID_Farmaco as FarmacoId, Proveedor, ID_Fecha as Fecha, PVAlb as PVAlbaran, PC FROM Recepcion WHERE ID_Fecha >= #{fecha.ToString("MM-dd-yyyy HH:mm:ss")}# GROUP BY ID_Farmaco, Proveedor, ID_Fecha, PVAlb, PC ORDER BY ID_Fecha DESC";

                rs = db.Database.SqlQuery<DTO.ProveedorHistorico>(sql)
                    .Where(r => r.Fecha.HasValue)
                    .Where(r => r.Proveedor.HasValue)
                    .ToList();
            }

            return rs.Select(x => new DE.ProveedorHistorico
            {
                Id = x.Proveedor.Value,
                FarmacoId = x.Farmaco,
                Fecha = x.Fecha.Value,
                PUC = x.PC > 0
                    ? x.PC * _factorCentecimal
                    : x.PVAlbaran > 0
                        ? x.PVAlbaran * _factorCentecimal
                        : 0m
            });
        }

        public IEnumerable<DE.Recepcion> GetAllByDate(DateTime fecha)
        {
            throw new NotImplementedException();
        }
    }
}