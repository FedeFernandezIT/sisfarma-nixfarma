using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia.DTO;
using System;
using System.Collections.Generic;

using DC = Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;

using DE = Sisfarma.Sincronizador.Domain.Entities.Farmacia;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public interface IRecepcionRespository
    {
    }

    public class RecepcionRespository : FarmaciaRepository, IRecepcionRespository, DC.IRecepcionRepository
    {
        private readonly IProveedorRepository _proveedorRepository;
        private readonly IFarmacoRepository _farmacoRepository;
        private readonly ICategoriaRepository _categoriaRepository;
        private readonly IFamiliaRepository _familiaRepository;
        private readonly ILaboratorioRepository _laboratorioRepository;

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

                    reader.Close();
                    reader.Dispose();

                    return new RecepcionTotales
                    {
                        Lineas = rNumLineas,
                        PVP = rImportePvp,
                        PUC = rImportePuc
                    };
                }

                reader.Close();
                reader.Dispose();

                return new RecepcionTotales();
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
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

                reader.Close();
                reader.Dispose();

                return recepciones;
            }
            catch (Exception ex)
            {
                throw;
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

                reader.Close();
                reader.Dispose();

                return recepciones;
            }
            catch (Exception ex)
            {
                throw;
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

        public IEnumerable<DE.ProveedorHistorico> GetAllHistoricosByFecha(DateTime fecha)
        {
            var historicos = new List<DE.ProveedorHistorico>();
            var conn = FarmaciaContext.GetConnection();
            try
            {
                conn.Open();
                var sql = $@"
                    SELECT art_codigo, proveedor, fecha_pedido, pc_iva_euros
                    FROM appul.ad_rec_linped WHERE to_char(fecha_pedido, 'YYYYMMDDHH24MISS') > '{fecha.ToString("yyyyMMddHHmmss")}'
                    GROUP BY art_codigo, proveedor, fecha_pedido, pc_iva_euros
                    ORDER BY fecha_pedido DESC";

                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var rArtCodigo = Convert.ToString(reader["art_codigo"]);
                    var rProveedor = !Convert.IsDBNull(reader["proveedor"]) ? Convert.ToInt64(reader["proveedor"]) : 0L;
                    var rFechaPedido = !Convert.IsDBNull(reader["fecha_pedido"]) ? Convert.ToDateTime(reader["fecha_pedido"]) : DateTime.MinValue;
                    var rPcIvaEuros = !Convert.IsDBNull(reader["pc_iva_euros"]) ? Convert.ToDecimal(reader["pc_iva_euros"]) : 0m;

                    var historico = new DE.ProveedorHistorico
                    {
                        Id = rProveedor,
                        FarmacoId = rArtCodigo,
                        Fecha = rFechaPedido,
                        PUC = rPcIvaEuros
                    };

                    historicos.Add(historico);
                }

                reader.Close();
                reader.Dispose();

                return historicos;
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        public IEnumerable<DE.Recepcion> GetAllByDate(DateTime fecha)
        {
            throw new NotImplementedException();
        }
    }
}