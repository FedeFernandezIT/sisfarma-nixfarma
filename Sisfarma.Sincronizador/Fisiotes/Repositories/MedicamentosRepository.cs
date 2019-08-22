using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Fisiotes.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Fisiotes.Repositories
{
    public class MedicamentosRepository : FisiotesRepository
    {
        public MedicamentosRepository(FisiotesContext ctx) : base(ctx)
        {
        }

        public MedicamentosRepository(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        {
        }

        public IEnumerable<Medicamento> GetGreaterOrEqualCodigosNacionales(string codigo)
        {
            if (string.IsNullOrEmpty(codigo))            
                codigo = "0";            
            try
            {                
                return _restClient
                    .Resource(_config.Medicamentos
                        .GetGreaterOrEqualByCodigoNacional
                            .Replace("{id}", codigo)
                            .Replace("{limit}", $"{1000}")
                            .Replace("{order}", "asc"))
                    .SendGet<IEnumerable<Medicamento>>();
            }
            catch (RestClientNotFoundException)
            {
                return new List<Medicamento>();
            }            
        }

        public void DeleteByCodigoNacional(string codigo)
        {
            _restClient
                .Resource(_config.Medicamentos.Delete)
                .SendPut(new
                {
                    id = codigo
                });            
        }

        public void ResetPorDondeVoySinStock()
        {
            _restClient
                .Resource(_config.Medicamentos.ResetSeguimientoSinStock)
                .SendPut();
        }

        public void ResetPorDondeVoy()
        {
            _restClient
                .Resource(_config.Medicamentos.ResetSeguimientoDondeVoy)
                .SendPut();
        }

        public Medicamento GetOneOrDefaultByCodNacional(string codNacional)
        {
            try
            {
                return _restClient
                    .Resource(_config.Medicamentos
                        .GetByCodNacional
                            .Replace("{id}", codNacional))                            
                    .SendGet<Medicamento>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public void Insert(Medicamento mm)
        {
            var medicamento = new[] { new
                {
                    actualizadoPS = 1,
                    cod_barras = mm.cod_barras,
                    cod_nacional = mm.cod_nacional,
                    nombre = mm.nombre,
                    superFamilia = mm.superFamilia,
                    familia = mm.familia,
                    precio = mm.precio,
                    descripcion = mm.descripcion,
                    laboratorio = mm.laboratorio,
                    nombre_laboratorio = mm.nombre_laboratorio,
                    proveedor = mm.proveedor,
                    pvpSinIva = mm.pvpSinIva,
                    iva = mm.iva,
                    stock = mm.stock,
                    puc = mm.puc,
                    stockMinimo = mm.stockMinimo,
                    stockMaximo = mm.stockMaximo,
                    presentacion = mm.presentacion,
                    descripcionTienda = mm.descripcionTienda,
                    activoPrestashop = mm.activoPrestashop.ToInteger(),
                    fechaCaducidad = mm.fechaCaducidad.ToIsoString(),
                    fechaUltimaCompra = mm.fechaUltimaCompra.ToIsoString(),
                    fechaUltimaVenta = mm.fechaUltimaVenta.ToIsoString(),
                    baja = mm.baja.ToInteger()
                }};

            _restClient.
                Resource(_config.Medicamentos.Insert)
                .SendPost(new { bulk = medicamento });
        }

        public void Insert(string codigoBarras, string codNacional, string nombre, string superFamilia, string familia,
            float precio, string descripcion, string laboratorio, string nombreLaboratorio, string proveedor, float pvpSinIva, int iva,
            int stock, float puc, int stockMinimo, int stockMaximo, string presentacion, string descripcionTienda, bool activo, DateTime? caducidad,
            DateTime? ultimaCompra, DateTime? ultimaVenta, bool baja)
        {
            var medicamento = new[] { new
                {
                    actualizadoPS = 1,
                    cod_barras = codigoBarras,
                    cod_nacional = codNacional,
                    nombre = nombre,
                    superFamilia = superFamilia,
                    familia = familia,
                    precio = precio,
                    descripcion = descripcion,
                    laboratorio = laboratorio,
                    nombre_laboratorio = nombreLaboratorio,
                    proveedor = proveedor,
                    pvpSinIva = pvpSinIva,
                    iva = iva,
                    stock = stock,
                    puc = puc,
                    stockMinimo = stockMinimo,
                    stockMaximo = stockMaximo,
                    presentacion = presentacion,
                    descripcionTienda = descripcionTienda,
                    activoPrestashop = activo.ToInteger(),                    
                    fechaCaducidad = caducidad.ToIsoString(),
                    fechaUltimaCompra = ultimaCompra.ToIsoString(),
                    fechaUltimaVenta = ultimaVenta.ToIsoString(),
                    baja = baja.ToInteger()
                }};

            _restClient.
                Resource(_config.Medicamentos.Insert)
                .SendPost(new { bulk = medicamento });            
        }


        public void Update(Medicamento mm, bool withSqlExtra = false)
        {
            var medicamento = (withSqlExtra) 
                ? GenerarMedicamentoAnonymusWhithoutExta(mm)
                : GenerarMedicamentoAnonymusWithExtra(mm);
            
            _restClient.
                Resource(_config.Medicamentos.Update)
                .SendPost(new { bulk = new[] { medicamento } });
        }

        private object GenerarMedicamentoAnonymusWithExtra(Medicamento mm)
        {
            return new
                {
                    cargadoPS = 0,
                    actualizadoPS = 1,
                    cod_barras = mm.cod_barras,
                    cod_nacional = mm.cod_nacional,
                    nombre = mm.nombre,
                    superFamilia = mm.superFamilia,
                    familia = mm.familia,
                    precio = mm.precio,
                    descripcion = mm.descripcion,
                    laboratorio = mm.laboratorio,
                    nombre_laboratorio = mm.nombre_laboratorio,
                    proveedor = mm.proveedor,
                    pvpSinIva = mm.pvpSinIva,
                    iva = mm.iva,
                    stock = mm.stock,
                    puc = mm.puc,
                    stockMinimo = mm.stockMinimo,
                    stockMaximo = mm.stockMaximo,
                    presentacion = mm.presentacion,
                    descripcionTienda = mm.descripcionTienda,
                    activoPrestashop = mm.activoPrestashop.ToInteger(),
                    fechaCaducidad = mm.fechaCaducidad.ToIsoString(),
                    fechaUltimaCompra = mm.fechaUltimaCompra.ToIsoString(),
                    fechaUltimaVenta = mm.fechaUltimaVenta.ToIsoString(),
                    baja = mm.baja.ToInteger()
                };
        }

        private object GenerarMedicamentoAnonymusWhithoutExta(Medicamento mm)
        {
            return new
                {
                    cod_barras = mm.cod_barras,
                    cod_nacional = mm.cod_nacional,
                    nombre = mm.nombre,
                    superFamilia = mm.superFamilia,
                    familia = mm.familia,
                    precio = mm.precio,
                    descripcion = mm.descripcion,
                    laboratorio = mm.laboratorio,
                    nombre_laboratorio = mm.nombre_laboratorio,
                    proveedor = mm.proveedor,
                    pvpSinIva = mm.pvpSinIva,
                    iva = mm.iva,
                    stock = mm.stock,
                    puc = mm.puc,
                    stockMinimo = mm.stockMinimo,
                    stockMaximo = mm.stockMaximo,
                    presentacion = mm.presentacion,
                    descripcionTienda = mm.descripcionTienda,
                    activoPrestashop = mm.activoPrestashop.ToInteger(),
                    fechaCaducidad = mm.fechaCaducidad.ToIsoString(),
                    fechaUltimaCompra = mm.fechaUltimaCompra.ToIsoString(),
                    fechaUltimaVenta = mm.fechaUltimaVenta.ToIsoString(),
                    baja = mm.baja.ToInteger()
                };
        }

        public void Update(string codigoBarras, string nombre, string superFamilia, string familia,
            float precio, string descripcion, string laboratorio, string nombreLaboratorio, string proveedor,
            int iva, float pvpSinIva, int stock, float puc, int stockMinimo, int stockMaximo, string presentacion,
            string descripcionTienda, bool activo, DateTime? caducidad, DateTime? ultimaCompra, DateTime? ultimaVenta,
            bool baja, string codNacional, bool withSqlExtra = false)
        {
            object medicamento;

            if(!withSqlExtra)
                medicamento = new[] { new
                {                    
                    cod_barras = codigoBarras,
                    cod_nacional = codNacional,
                    nombre = nombre,
                    superFamilia = superFamilia,
                    familia = familia,
                    precio = precio,
                    descripcion = descripcion,
                    laboratorio = laboratorio,
                    nombre_laboratorio = nombreLaboratorio,
                    proveedor = proveedor,
                    pvpSinIva = pvpSinIva,
                    iva = iva,
                    stock = stock,
                    puc = puc,
                    stockMinimo = stockMinimo,
                    stockMaximo = stockMaximo,
                    presentacion = presentacion,
                    descripcionTienda = descripcionTienda,
                    activoPrestashop = activo.ToInteger(),
                    fechaCaducidad = caducidad.ToIsoString(),
                    fechaUltimaCompra = ultimaCompra.ToIsoString(),
                    fechaUltimaVenta = ultimaVenta.ToIsoString(),
                    baja = baja.ToInteger()
                }};
            else
                medicamento = new[] { new
                {
                    cargadoPS = 0,
                    actualizadoPS = 1,
                    cod_barras = codigoBarras,
                    cod_nacional = codNacional,
                    nombre = nombre,
                    superFamilia = superFamilia,
                    familia = familia,
                    precio = precio,
                    descripcion = descripcion,
                    laboratorio = laboratorio,
                    nombre_laboratorio = nombreLaboratorio,
                    proveedor = proveedor,
                    pvpSinIva = pvpSinIva,
                    iva = iva,
                    stock = stock,
                    puc = puc,
                    stockMinimo = stockMinimo,
                    stockMaximo = stockMaximo,
                    presentacion = presentacion,
                    descripcionTienda = descripcionTienda,
                    activoPrestashop = activo.ToInteger(),
                    fechaCaducidad = caducidad.ToIsoString(),
                    fechaUltimaCompra = ultimaCompra.ToIsoString(),
                    fechaUltimaVenta = ultimaVenta.ToIsoString(),
                    baja = baja.ToInteger()
                }};



            _restClient.
                Resource(_config.Medicamentos.Update)
                .SendPost(new { bulk = medicamento });            
        }

        public bool HasWebField()
        {
            var existField = false;

            using (var ctx = new FisiotesContext())
            {
                // Chekeamos si existen los campos
                var connection = ctx.Database.Connection;
                var sql = @"SELECT * from medicamentos LIMIT 0,1;";
                var command = connection.CreateCommand();
                command.CommandText = sql;
                connection.Open();
                var reader = command.ExecuteReader();
                var schemaTable = reader.GetSchemaTable();

                foreach (DataRow row in schemaTable.Rows)
                {
                    if (row[schemaTable.Columns["ColumnName"]].ToString()
                        .Equals("web", StringComparison.CurrentCultureIgnoreCase))
                    {
                        existField = true;
                        break;
                    }
                }
                connection.Close();
            }
            return existField;
        }        

                

        

        public void Insert(string codigoBarras, string codNacional, string nombre, string superFamilia, string familia,
            float precio, string descripcion, string laboratorio, string nombreLaboratorio, string proveedor, float pvpSinIva, int iva,
            int stock, float puc, int stockMinimo, int stockMaximo, string presentacion, string descripcionTienda, bool activo, bool baja)
        {
            var sql = @"INSERT IGNORE INTO medicamentos (cod_barras, cod_nacional, nombre, superFamilia, familia,precio, descripcion, laboratorio, nombre_laboratorio, " +
                    @"proveedor, pvpSinIva, iva,stock, puc, stockMinimo, stockMaximo, presentacion, descripcionTienda, activoPrestashop, actualizadoPS, " +
                    @"baja) " +
                @"VALUES(@codigoBarras, @codNacional, @nombre, @superFamilia, @familia, @precio, @descripcion, @laboratorio, @nombreLaboratorio, " +
                    @"@proveedor, @pvpSinIva, @iva, @stock, @puc, @stockMinimo, @stockMaximo, @presentacion, @descripcionTienda, @activo, 1, " +
                    @"@baja)";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("codigoBarras", codigoBarras),
                new SqlParameter("codNacional", codNacional),
                new SqlParameter("nombre", nombre),
                new SqlParameter("superFamilia", superFamilia),
                new SqlParameter("familia", familia),
                new SqlParameter("precio", precio),
                new SqlParameter("descripcion", descripcion),
                new SqlParameter("laboratorio", laboratorio),
                new SqlParameter("nombreLaboratorio", nombreLaboratorio),
                new SqlParameter("proveedor", proveedor),
                new SqlParameter("iva", iva),
                new SqlParameter("pvpSinIva", pvpSinIva),
                new SqlParameter("stock", stock),
                new SqlParameter("puc", puc),
                new SqlParameter("stockMinimo", stockMinimo),
                new SqlParameter("stockMaximo", stockMaximo),
                new SqlParameter("presentacion", presentacion),
                new SqlParameter("descripcionTienda", descripcionTienda),
                new SqlParameter("activo", activo),
                new SqlParameter("baja", baja));
        }        

        public void Update(string codigoBarras, string nombre, string superFamilia, string familia,
            float precio, string descripcion, string laboratorio, string nombreLaboratorio, string proveedor,
            int iva, float pvpSinIva, int stock, float puc, int stockMinimo, int stockMaximo, string presentacion,
            string descripcionTienda, bool activo, bool baja, string codNacional)
        {
            var sql = @"UPDATE IGNORE medicamentos SET cod_barras = @codigoBarras, nombre = @nombre, superFamilia = @superFamilia, familia = @familia, " +
                @"precio = @precio, descripcion = @descripcion, laboratorio = @laboratorio, nombre_laboratorio = @nombreLaboratorio, proveedor = @proveedor," +
                @"iva = @iva, pvpSinIva = @pvpSinIva, stock = @stock, puc = @puc, stockMinimo = @stockMinimo, stockMaximo = @stockMaximo, " +
                @"presentacion = @presentacion, descripcionTienda = @descripcionTienda, cargadoPS = 0, actualizadoPS = 1, " +
                @" activoPrestashop = @activo, baja = @baja WHERE cod_nacional = @codNacional";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("codigoBarras", codigoBarras),
                new SqlParameter("nombre", nombre),
                new SqlParameter("superFamilia", superFamilia),
                new SqlParameter("familia", familia),
                new SqlParameter("precio", precio),
                new SqlParameter("descripcion", descripcion),
                new SqlParameter("laboratorio", laboratorio),
                new SqlParameter("nombreLaboratorio", nombreLaboratorio),
                new SqlParameter("proveedor", proveedor),
                new SqlParameter("iva", iva),
                new SqlParameter("pvpSinIva", pvpSinIva),
                new SqlParameter("stock", stock),
                new SqlParameter("puc", puc),
                new SqlParameter("stockMinimo", stockMinimo),
                new SqlParameter("stockMaximo", stockMaximo),
                new SqlParameter("presentacion", presentacion),
                new SqlParameter("descripcionTienda", descripcionTienda),
                new SqlParameter("activo", activo),
                new SqlParameter("baja", baja),
                new SqlParameter("codNacional", codNacional));
        }
                

        public void CheckAndCreateFields()
        {
            const string table = @"SELECT * from medicamentos LIMIT 0,1;";
            var fields = new[]
            {
                "laboratorio", "stock", "puc", "stockMinimo", "presentacion", "fechaCaducidad",
                "porDondeVoySinStock", "fechaUltimaCompra", "proveedor", "superFamilia"
            };
            var alters = new[]
            {
                @"ALTER TABLE medicamentos ADD laboratorio VARCHAR(255);",
                @"ALTER TABLE medicamentos ADD (pvpSinIva float, iva int (11), stock int (11));",
                @"ALTER TABLE medicamentos ADD (puc float);",
                @"ALTER TABLE medicamentos ADD (stockMinimo int (11), stockMaximo int (11));",
                @"ALTER TABLE medicamentos ADD `nombre_laboratorio` varchar(255) DEFAULT NULL AFTER laboratorio, " +
                    @"ADD (`presentacion` varchar(50) DEFAULT NULL, `descripcionTienda` text, `prestashopIdPS` int(10) DEFAULT NULL, " +
                    @"`cargadoPS` tinyint(1) DEFAULT '0', `fechaCargadoPS` datetime DEFAULT NULL, `activoPrestashop` tinyint(1) DEFAULT '1', " +
                    @"`actualizadoPS` tinyint(1) DEFAULT '0', `eliminado` tinyint(1) DEFAULT '0', `fechaEliminado` datetime DEFAULT NULL);",
                @"ALTER TABLE medicamentos ADD (fechaCaducidad datetime, porDondeVoy TINYINT(1) DEFAULT 0);",
                @"ALTER TABLE medicamentos ADD (porDondeVoySinStock TINYINT(1) DEFAULT 0);",
                @"ALTER TABLE medicamentos ADD (fechaUltimaCompra DATETIME DEFAULT NULL, fechaUltimaVenta DATETIME DEFAULT NULL);",
                @"ALTER TABLE medicamentos ADD proveedor VARCHAR(255) DEFAULT NULL AFTER nombre_laboratorio, " +
                    @"ADD (baja TINYINT(1) DEFAULT 0);",
                @"ALTER TABLE medicamentos ADD superFamilia VARCHAR(255) DEFAULT NULL AFTER nombre;"
            };
            CheckAndCreateFieldsTemplate(table, fields, alters);
        }

        #region SQL Methods

        public List<string> GetCodigosNacionalesGreaterOrEqualSql(string codigo, bool withWeb = false)
        {
            var sql = withWeb
                ? @"SELECT cod_nacional FROM medicamentos WHERE web = 0 AND cod_nacional >= @codigo ORDER BY cod_nacional ASC LIMIT 0,1000"
                : @"SELECT cod_nacional FROM medicamentos WHERE cod_nacional >= @codigo ORDER BY cod_nacional ASC LIMIT 0,1000";
            return _ctx.Database.SqlQuery<string>(sql,
                new SqlParameter("codigo", codigo))
                .ToList();
        }

        public void DeleteByCodigoNacionalSql(string codigo)
        {
            var sql = @"DELETE FROM medicamentos WHERE cod_nacional = @codigo";
            _ctx.Database.ExecuteSqlCommand(sql, new SqlParameter("codigo", codigo));
        }

        public void ResetPorDondeVoySinStockSql()
        {
            var sql = @"UPDATE IGNORE medicamentos SET porDondeVoySinStock = 0";
            _ctx.Database.ExecuteSqlCommand(sql);
        }

        public Medicamento GetByCodNacionalSql(string codNacional)
        {
            var sql = @"select * from medicamentos where cod_nacional = @codNacional";
            return _ctx.Database.SqlQuery<Medicamento>(sql,
                new SqlParameter("codNacional", codNacional))
                .FirstOrDefault();
        }

        public void InsertSql(string codigoBarras, string codNacional, string nombre, string superFamilia, string familia,
            float precio, string descripcion, string laboratorio, string nombreLaboratorio, string proveedor, float pvpSinIva, int iva,
            int stock, float puc, int stockMinimo, int stockMaximo, string presentacion, string descripcionTienda, bool activo, DateTime? caducidad,
            DateTime? ultimaCompra, DateTime? ultimaVenta, bool baja)
        {
            var sql = @"INSERT INTO medicamentos (cod_barras, cod_nacional, nombre, superFamilia, familia,precio, descripcion, laboratorio, nombre_laboratorio, " +
                    @"proveedor, pvpSinIva, iva,stock, puc, stockMinimo, stockMaximo, presentacion, descripcionTienda, activoPrestashop, actualizadoPS, " +
                    @"fechaCaducidad, fechaUltimaCompra, fechaUltimaVenta,baja) " +
                @"VALUES(@codigoBarras, @codNacional, @nombre, @superFamilia, @familia, @precio, @descripcion, @laboratorio, @nombreLaboratorio, " +
                    @"@proveedor, @pvpSinIva, @iva, @stock, @puc, @stockMinimo, @stockMaximo, @presentacion, @descripcionTienda, @activo, 1, " +
                    @"@caducidad, @ultimaCompra, @ultimaVenta, @baja)";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("codigoBarras", codigoBarras),
                new SqlParameter("codNacional", codNacional),
                new SqlParameter("nombre", nombre),
                new SqlParameter("superFamilia", superFamilia),
                new SqlParameter("familia", familia),
                new SqlParameter("precio", precio),
                new SqlParameter("descripcion", descripcion),
                new SqlParameter("laboratorio", laboratorio),
                new SqlParameter("nombreLaboratorio", nombreLaboratorio),
                new SqlParameter("proveedor", proveedor),
                new SqlParameter("iva", iva),
                new SqlParameter("pvpSinIva", pvpSinIva),
                new SqlParameter("stock", stock),
                new SqlParameter("puc", puc),
                new SqlParameter("stockMinimo", stockMinimo),
                new SqlParameter("stockMaximo", stockMaximo),
                new SqlParameter("presentacion", presentacion),
                new SqlParameter("descripcionTienda", descripcionTienda),
                new SqlParameter("activo", activo),
                new SqlParameter("caducidad", caducidad),
                new SqlParameter("ultimaCompra", ultimaCompra),
                new SqlParameter("ultimaVenta", ultimaVenta),
                new SqlParameter("baja", baja));
        }

        public void UpdateSql(string codigoBarras, string nombre, string superFamilia, string familia,
            float precio, string descripcion, string laboratorio, string nombreLaboratorio, string proveedor,
            int iva, float pvpSinIva, int stock, float puc, int stockMinimo, int stockMaximo, string presentacion,
            string descripcionTienda, bool activo, DateTime? caducidad, DateTime? ultimaCompra, DateTime? ultimaVenta,
            bool baja, string codNacional, bool withSqlExtra = false)
        {
            var sqlExtra = withSqlExtra ? string.Empty : " cargadoPS = 0, actualizadoPS = 1, ";
            var sql = @"UPDATE IGNORE medicamentos SET cod_barras = @codigoBarras, nombre = @nombre, superFamilia = @superFamilia, familia = @familia, " +
                    @"precio = @precio, descripcion = @descripcion, laboratorio = @laboratorio, nombre_laboratorio = @nombreLaboratorio, proveedor = @proveedor," +
                    @"iva = @iva, pvpSinIva = @pvpSinIva, stock = @stock, puc = @puc, stockMinimo = @stockMinimo, stockMaximo = @stockMaximo, " +
                    @"presentacion = @presentacion, descripcionTienda = @descripcionTienda, " + sqlExtra +
                    @" activoPrestashop = @activo, fechaCaducidad = @caducidad, fechaUltimaCompra = @ultimaCompra, fechaUltimaVenta = @ultimaVenta, " +
                    @"baja = @baja WHERE cod_nacional = @codNacional";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("codigoBarras", codigoBarras),
                new SqlParameter("nombre", nombre),
                new SqlParameter("superFamilia", superFamilia),
                new SqlParameter("familia", familia),
                new SqlParameter("precio", precio),
                new SqlParameter("descripcion", descripcion),
                new SqlParameter("laboratorio", laboratorio),
                new SqlParameter("nombreLaboratorio", nombreLaboratorio),
                new SqlParameter("proveedor", proveedor),
                new SqlParameter("iva", iva),
                new SqlParameter("pvpSinIva", pvpSinIva),
                new SqlParameter("stock", stock),
                new SqlParameter("puc", puc),
                new SqlParameter("stockMinimo", stockMinimo),
                new SqlParameter("stockMaximo", stockMaximo),
                new SqlParameter("presentacion", presentacion),
                new SqlParameter("descripcionTienda", descripcionTienda),
                new SqlParameter("activo", activo),
                new SqlParameter("caducidad", caducidad),
                new SqlParameter("ultimaCompra", ultimaCompra),
                new SqlParameter("ultimaVenta", ultimaVenta),
                new SqlParameter("baja", baja),
                new SqlParameter("codNacional", codNacional));
        }

        public void ResetPorDondeVoySql()
        {
            var sql = @"UPDATE IGNORE medicamentos SET porDondeVoy = 0";
            _ctx.Database.ExecuteSqlCommand(sql);
        }

        #endregion
    }
}