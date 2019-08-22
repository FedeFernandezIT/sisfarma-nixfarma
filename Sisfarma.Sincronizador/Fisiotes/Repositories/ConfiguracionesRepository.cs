using Sisfarma.RestClient;
using Sisfarma.Sincronizador.Fisiotes.Models;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Fisiotes.Repositories
{
    public class ConfiguracionesRepository : FisiotesRepository
    {
        public ConfiguracionesRepository(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        {
        }

        public string GetByCampo(string field)
        {
            return _restClient
                .Resource(_config.Configuraciones.GetValorByCampo.Replace("{campo}", field))
                .SendGet<string>();
        }

        public void Update(string field, string value)
        {
            _restClient
                .Resource(_config.Configuraciones.UpdateValorByCampo)
                .SendPut(new { campo = field, valor = value });
        }

        public bool PerteneceFarmazul()
        {
            return _restClient
               .Resource(_config.Configuraciones.PerteneceFarmazul)
               .SendGet<bool>();
        }

        public IEnumerable<Configuracion> GetEstadosActuales()
        {
            return _restClient
               .Resource(_config.Configuraciones.GetAll)
               .SendGet<IEnumerable<Configuracion>>();
        }

        public void Insert(string field)
        {
            var sql = string.Empty;
            switch (field)
            {
                case FieldsConfiguracion.FIELD_STOCK_ENTRADA:
                case FieldsConfiguracion.FIELD_STOCK_SALIDA:
                    sql = @"INSERT IGNORE INTO configuracion (campo, valor) VALUES (@field, NULL)";
                    break;

                case FieldsConfiguracion.FIELD_POR_DONDE_VOY_CON_STOCK:
                case FieldsConfiguracion.FIELD_POR_DONDE_VOY_SIN_STOCK:
                case FieldsConfiguracion.FIELD_POR_DONDE_VOY_BORRAR:
                case FieldsConfiguracion.FIELD_POR_DONDE_VOY_ENTREGAS_CLIENTES:
                    sql = @"INSERT IGNORE INTO configuracion (campo, valor) VALUES (@field, '0')";
                    break;
            }
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("field", field));
        }

        public static class FieldsConfiguracion
        {
            public const string FIELD_STOCK_ENTRADA = "fechaActualizacionStockEntrada";
            public const string FIELD_STOCK_SALIDA = "fechaActualizacionStockSalida";
            public const string FIELD_POR_DONDE_VOY_CON_STOCK = "porDondeVoyConStock";
            public const string FIELD_POR_DONDE_VOY_SIN_STOCK = "porDondeVoySinStock";
            public const string FIELD_POR_DONDE_VOY_BORRAR = "porDondeVoyBorrar";
            public const string FIELD_POR_DONDE_VOY_ENTREGAS_CLIENTES = "porDondeEntregasClientes";
            public const string FIELD_POR_DONDE_VOY_VENTA_MES = "porDondeVoyActualizarVentasMes";
            public const string FIELD_POR_DONDE_VOY_VENTA_MES_ID = "porDondeVoyActualizarVentasMesIdVenta";
            public const string FIELD_REVISAR_VENTA_MES_DESDE = "revisarVentasDesdeMeses";
            public const string FIELD_FECHA_PUNTOS = "fechaPuntos";
            public const string FIELD_CARGAR_PUNTOS = "cargarPuntos";
            public const string FIELD_SOLO_PUNTOS_CON_TARJETA = "soloPuntosConTarjeta";
            public const string FIELD_CANJEO_PUNTOS = "canjeoPuntos";
            public const string FIELD_LOG_ERRORS = "logErrors";
            public const string FIELD_ENCENDIDO = "estadoSincro";
            public const string FIELD_ANIO_INICIO = "anioInicioSincro";
            public const string FIELD_PUNTOS_SISFARMA = "puntosPorSisfarma";
            public const string FIELD_COPIAS_CLIENTES = "copiarClientes";
        }

        #region SQL Methods

        public Configuracion GetByCampoSql(string field)
        {
            var sql = @"SELECT * FROM configuracion WHERE campo = @field";
            return _ctx.Database.SqlQuery<Configuracion>(sql,
                new SqlParameter("field", field))
                .FirstOrDefault();
        }

        public void UpdateSql(string field, string value)
        {
            var sql = @"UPDATE IGNORE configuracion SET valor = @value WHERE campo = @field";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("value", value),
                new SqlParameter("field", field));
        }

        #endregion SQL Methods
    }
}