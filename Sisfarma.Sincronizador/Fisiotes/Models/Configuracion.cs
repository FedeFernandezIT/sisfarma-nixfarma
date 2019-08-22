namespace Sisfarma.Sincronizador.Fisiotes.Models
{
    public partial class Configuracion
    {
        public ulong id { get; set; }

        public string campo { get; set; }

        public string valor { get; set; }

        public string activo { get; set; }

        public static readonly string FIELD_STOCK_ENTRADA = "fechaActualizacionStockEntrada";
        public static readonly string FIELD_STOCK_SALIDA = "fechaActualizacionStockSalida";
        public static readonly string FIELD_POR_DONDE_VOY_CON_STOCK = "porDondeVoyConStock";
        public static readonly string FIELD_POR_DONDE_VOY_SIN_STOCK = "porDondeVoySinStock";
        public static readonly string FIELD_POR_DONDE_VOY_BORRAR = "porDondeVoyBorrar";
        public static readonly string FIELD_POR_DONDE_VOY_ENTREGAS_CLIENTES = "porDondeEntregasClientes";
        public static readonly string FIELD_POR_DONDE_VOY_VENTA_MES = "porDondeVoyActualizarVentasMes";
        public static readonly string FIELD_POR_DONDE_VOY_VENTA_MES_ID = "porDondeVoyActualizarVentasMesIdVenta";
        public static readonly string FIELD_REVISAR_VENTA_MES_DESDE = "revisarVentasDesdeMeses";
        public static readonly string FIELD_FECHA_PUNTOS = "fechaPuntos";
        public static readonly string FIELD_CARGAR_PUNTOS = "cargarPuntos";
        public static readonly string FIELD_SOLO_PUNTOS_CON_TARJETA = "soloPuntosConTarjeta";
        public static readonly string FIELD_CANJEO_PUNTOS = "canjeoPuntos";
        public static readonly string FIELD_LOG_ERRORS = "logErrors";
        public static readonly string FIELD_ENCENDIDO = "estadoSincro";
        public static readonly string FIELD_ANIO_INICIO = "anioInicioSincro";
        public static readonly string FIELD_PUNTOS_SISFARMA = "puntosPorSisfarma";
        public static readonly string FIELD_COPIAS_CLIENTES = "copiarClientes";
        public static readonly string FIELD_ES_FARMAZUL = "esFarmazul";
    }
}