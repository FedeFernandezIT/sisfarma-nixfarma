using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Linq;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public class PedidosRepository : FarmaciaRepository, IPedidosRepository
    {
        private readonly IProveedorRepository _proveedorRepository;
        private readonly IFarmacoRepository _farmacoRepository;
        private readonly ICategoriaRepository _categoriaRepository;
        private readonly IFamiliaRepository _familiaRepository;
        private readonly ILaboratorioRepository _laboratorioRepository;

        private readonly decimal _factorCentecimal = 0.01m;

        public PedidosRepository(LocalConfig config) : base(config)
        { }

        public PedidosRepository(
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

        public IEnumerable<Pedido> GetAllByFechaGreaterOrEqual(DateTime fecha)
        {
            var pedidos = new List<Pedido>();
            var conn = FarmaciaContext.GetConnection();
            try
            {
                var sqlExtra = string.Empty;
                var sql = $@"
                    SELECT * From appul.ad_pedidos WHERE to_char(fecha_pedido, 'YYYYMMDD') >= '{fecha.ToString("yyyyMMdd")}' Order by pedido ASC";

                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var rFechaPedido = Convert.ToDateTime(reader["FECHA_PEDIDO"]);
                    var rPedido = Convert.ToInt64(reader["PEDIDO"]);
                    var rEmpCodigo = Convert.ToString(reader["EMP_CODIGO"]);

                    var pedido = new Pedido
                    {
                        Fecha = rFechaPedido,
                        Numero = rPedido,
                        Empresa = rEmpCodigo
                    };

                    pedidos.Add(pedido);
                }

                return pedidos;
            }
            catch (Exception ex)
            {
                return pedidos;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        internal class PedidoCompositeKey
        {
            internal short Id { get; set; }

            internal int Proveedor { get; set; }
        }

        public IEnumerable<Pedido> GetAllByIdGreaterOrEqual(long pedido)
        {
            var rs = Enumerable.Empty<DTO.Pedido>();
            using (var db = FarmaciaContext.Proveedores())
            {
                var sql = @"SELECT ID_NumPedido as Id, ID_Proveedor as Proveedor, ID_Farmaco as Farmaco, CantInicial, Fecha From recibir WHERE ID_NumPedido >= @pedido Order by ID_NumPedido ASC";
                rs = db.Database.SqlQuery<DTO.Pedido>(sql,
                    new OleDbParameter("pedido", (int)pedido))
                        .Take(10)
                        .ToList();
            }

            var keys = rs.GroupBy(k => new PedidoCompositeKey { Id = k.Id, Proveedor = k.Proveedor });
            return GenerarPedidos(keys);
        }

        private IEnumerable<Pedido> GenerarPedidos(IEnumerable<IGrouping<PedidoCompositeKey, DTO.Pedido>> groups)
        {
            var pedidos = new List<Pedido>();
            foreach (var group in groups)
            {
                var linea = 0;
                var fecha = group.FirstOrDefault()?.Fecha;
                var detalle = new List<PedidoDetalle>();
                foreach (var item in group)
                {
                    var pedidoDetalle = new PedidoDetalle()
                    {
                        Linea = ++linea,
                        CantidadPedida = item.CantInicial,
                        PedidoId = item.Id
                    };

                    var farmaco = _farmacoRepository.GetOneOrDefaultById(item.Farmaco.ToString());
                    if (farmaco != null)
                    {
                        var pcoste = farmaco.PrecioUnicoEntrada.HasValue && farmaco.PrecioUnicoEntrada != 0
                            ? (decimal)farmaco.PrecioUnicoEntrada.Value * _factorCentecimal
                            : ((decimal?)farmaco.PrecioMedio ?? 0m) * _factorCentecimal;

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
                        var laboratorio = _laboratorioRepository.GetOneOrDefaultByCodigo(farmaco.Laboratorio.Value, null, null); // TODO check clase, clasebot

                        pedidoDetalle.Farmaco = new Farmaco
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
                            Precio = farmaco.PVP * _factorCentecimal,
                            Stock = farmaco.Existencias ?? 0
                        };
                    }
                    detalle.Add(pedidoDetalle);
                }
                pedidos.Add(new Pedido { Id = group.Key.Id, Fecha = fecha.Value }.AddRangeDetalle(detalle));
            }
            return pedidos;
        }

        public IEnumerable<PedidoDetalle> GetAllDetalleByPedido(long numero, string empresa, int anio)
        {
            var detalle = new List<PedidoDetalle>();
            var conn = FarmaciaContext.GetConnection();
            try
            {
                var sqlExtra = string.Empty;
                var sql = $@"
                    select * from appul.ad_linped where pedido='{numero}' AND emp_codigo ='{empresa}' and ejercicio = {anio}";

                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var rArtCodigo = Convert.ToString(reader["ART_CODIGO"]);
                    var rLinea = Convert.ToInt32(reader["LINEA"]);
                    var rCantPedida = !Convert.IsDBNull(reader["CANT_PEDIDA"]) ? Convert.ToInt64(reader["CANT_PEDIDA"]) : 0L;
                    var rPedido = Convert.ToInt64(reader["PEDIDO"]);
                    var rEmpCodigo = Convert.ToString(reader["EMP_CODIGO"]);

                    var rPvpIvaEuros = !Convert.IsDBNull(reader["PVP_IVA_EUROS"]) ? (decimal?)Convert.ToDecimal(reader["PVP_IVA_EUROS"]) : null;
                    var rPcIvaEuros = !Convert.IsDBNull(reader["PC_IVA_EUROS"]) ? (decimal?)Convert.ToDecimal(reader["PC_IVA_EUROS"]) : null;

                    var farmaco = _farmacoRepository.GetOneOrDefaultById(rArtCodigo);
                    Farmaco farmacoPedido = null;
                    if (farmaco != null)
                    {
                        sql = $@"select max(actuales) as stock from appul.ac_existencias where art_codigo = '{rArtCodigo}' group by art_codigo";
                        cmd.CommandText = sql;
                        var readerStock = cmd.ExecuteReader();
                        var stock = readerStock.Read() ? Convert.ToInt64(readerStock["stock"]) : 0L;
                        farmaco.Stock = stock;

                        var proveedor = _proveedorRepository.GetOneOrDefaultByCodigoNacional(rArtCodigo);
                        var categoria = _categoriaRepository.GetOneOrDefaultById(rArtCodigo);

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

                        farmacoPedido = new Farmaco
                        {
                            Id = farmaco.Id,
                            Codigo = farmaco.Codigo,
                            PrecioCoste = farmaco.PUC,
                            Proveedor = proveedor,
                            Categoria = categoria,
                            Familia = familia,
                            Laboratorio = laboratorio,
                            Denominacion = farmaco.Denominacion,
                            Precio = farmaco.PrecioMedio ?? 0,
                            Stock = farmaco.Existencias ?? 0
                        };
                    }

                    var item = new PedidoDetalle
                    {
                        Linea = rLinea,
                        CantidadPedida = rCantPedida,
                        FarmacoCodigo = rArtCodigo,
                        EmpresaCodigo = rEmpCodigo,
                        PedidoCodigo = rPedido,
                        Farmaco = farmacoPedido
                    };

                    detalle.Add(item);
                }

                return detalle;
            }
            catch (Exception ex)
            {
                return detalle;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        //public IEnumerable<Pedido> GetByIdGreaterOrEqual(long? pedido)
        //{
        //    using (var db = FarmaciaContext.Create(_config))
        //    {
        //        var sql = @"SELECT * From pedido WHERE IdPedido >= @pedido Order by IdPedido ASC";
        //        return db.Database.SqlQuery<Pedido>(sql,
        //            new SqlParameter("pedido", pedido ?? SqlInt64.Null))
        //            .ToList();
        //    }
        //}

        //public IEnumerable<Pedido> GetByFechaGreaterOrEqual(DateTime fecha)
        //{
        //    using (var db = FarmaciaContext.Create(_config))
        //    {
        //        var sql = @"SELECT * From pedido WHERE Fecha >= @fecha Order by IdPedido ASC";
        //        return db.Database.SqlQuery<Pedido>(sql,
        //            new SqlParameter("fecha", fecha))
        //            .ToList();
        //    }
        //}

        //public IEnumerable<LineaPedido> GetLineasByPedido(int pedido)
        //{
        //    using (var db = FarmaciaContext.Create(_config))
        //    {
        //        var sql = @"select * from lineaPedido where IdPedido = @pedido";
        //        return db.Database.SqlQuery<LineaPedido>(sql,
        //            new SqlParameter("pedido", pedido))
        //            .ToList();
        //    }
        //}
    }
}