namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class FisiotesConfig
    {
        public string BaseAddress { get; set; }

        public Credential Credentials { get; set; }

        public CategoriaResource Categorias { get; set; }

        public ClienteResource Clientes { get; set; }

        public HuecoResource Huecos { get; set; }

        public PuntoResource Puntos { get; set; }

        public ConfiguracionResource Configuraciones { get; set; }

        public EntregaResource Entregas { get; set; }

        public MedicamentoResource Medicamentos { get; set; }

        public SinonimoResource Sinonimos { get; set; }

        public PedidoResource Pedidos { get; set; }

        public FaltaResource Faltas { get; set; }

        public FamiliaResource Familias { get; set; }

        public EncargoResource Encargos { get; set; }

        public ListaResource Listas { get; set; }

        public ListaArticuloResource ListaDeArticulos { get; set; }

        public ProveedorResource Proveedores { get; set; }

        public ProgramacionResource Programacion { get; set; }

        public VentaResource Ventas { get; set; }

        public static FisiotesConfig TestConfig(string remoteServer, string token)
        {
            return new FisiotesConfig
            {
                BaseAddress = remoteServer,
                Credentials = new Credential
                {
                    Token = token
                },

                Clientes = new ClienteResource
                {
                    GetDniTrackingLast = "api/cliente/ultimo",
                    ResetDniTracking = "api/cliente/setCeroClientes",
                    GetByDni = "api/cliente/index/dni/{dni}",
                    Insert = "api/cliente/index/dni/{dni}",
                    InsertHueco = "api/cliente/index/dni/{dni}/esHueco/1",
                    Update = "api/cliente/update"
                },

                Huecos = new HuecoResource
                {
                    Exists = "api/cliente/exists/hueco/{hueco}",
                    Insert = "api/cliente/hueco",
                    GetAll = "api/cliente/hueco",
                    Delete = "api/cliente/huecoeliminar"
                },

                Puntos = new PuntoResource
                {
                    GetVentasNoActualizadas = "api/puntos/ventasNoActualizado/greateranio/{anio}",
                    GetVentasRecetasPendientes = "api/puntos/ventasRecetasPendientes/greateranio/{anio}",
                    GetSinRedencion = "api/puntos/sinRedencion",
                    GetLastOfYear = "api/puntos/ultimo/year/{year}",
                    GetByItemVenta = "api/puntos/item/venta/{venta}/linea/{linea}",
                    GetTimestampUltimaVenta = "api/puntos/ultimaFechaVenta/empresa/{empresa}",
                    GetPuntosByDni = "api/puntos/index/dni/{dni}",
                    GetPuntosCanjeadosByDni = "api/puntos/canjeados/dni/{dni}",
                    ExistsByFechaGreatThanOrEqual = "api/puntos/exists_greater_equal/ejercicio/{year}/fechaVenta/{fecha}/empresa/{empresa}",
                    Update = "api/puntos/update",
                    Insert = "api/puntos/createUpdate",
                    InsertActualizarVenta = "api/puntos/createUpdate/actualizarVenta/1",
                    Delete = "api/puntos/delete",
                    Exists = "api/puntos/exists/venta/{venta}"
                },

                Configuraciones = new ConfiguracionResource
                {
                    GetValorByCampo = "api/configuracion/index/campo/{campo}",
                    UpdateValorByCampo = "api/configuracion/campo",
                    PerteneceFarmazul = "api/configuracion/esfarmazul",
                    GetAll = "api/configuracion/all",
                    ConexionLocal = "api/configuracion/cliente"
                },

                Entregas = new EntregaResource
                {
                    GetByKey = "api/entregas/index/venta/{venta}/linea/{linea}",
                    Insert = "api/entregas/create"
                },

                Medicamentos = new MedicamentoResource
                {
                    GetGreaterOrEqualByCodigoNacional = "api/medicamentos/gteq/id/{id}/limit/{limit}/order/{order}",
                    GetByCodNacional = "api/medicamentos/index/id/{id}",
                    Delete = "api/medicamentos/eliminar",
                    ResetSeguimientoSinStock = "api/medicamentos/resetSinStock",
                    ResetSeguimientoDondeVoy = "api/medicamentos/resetDondeVoy",
                    Insert = "api/medicamentos/createUpdate",
                    Update = "api/medicamentos/createUpdate"
                },

                Sinonimos = new SinonimoResource
                {
                    IsEmpty = "api/sinonimos/isEmpty",
                    Empty = "api/sinonimos/empty",
                    Insert = "api/sinonimos/createUpdate"
                },

                Pedidos = new PedidoResource
                {
                    Ultimo = "api/pedido/ultimo",
                    GetByPedido = "api/pedido/index/idPedido/{pedido}",
                    Insert = "api/pedido/createUpdate",
                    GetByLineaDePedido = "api/linea/index/pedido/{pedido}/linea/{linea}",
                    InsertLineaDePedido = "api/linea/createUpdate"
                },

                Faltas = new FaltaResource
                {
                    Ultima = "api/faltas/ultimo",
                    GetByLineaDePedido = "api/faltas/index/pedido/{pedido}/linea/{linea}",
                    InsertLineaDePedido = "api/faltas/createUpdate"
                },

                Familias = new FamiliaResource
                {
                    GetByFamilia = "api/familia/cod/familia/{familia}",
                    GetPuntosByFamilia = "api/familia/puntos/familia/{familia}",
                    Insert = "api/familia/createUpdate"
                },

                Encargos = new EncargoResource
                {
                    Ultimo = "api/encargo/ultimo",
                    GetByEncargo = "api/encargo/index/encargo/{encargo}",
                    Insert = "api/encargo/createUpdate",
                    Update = "api/encargo/update"
                },

                Categorias = new CategoriaResource
                {
                    GetByCategoriaAndPadre = "api/categoria/index/categoria/{categoria}/padre/{padre}",
                    GetByPadre = "api/categoria/index/padre/{padre}",
                    Insert = "api/categoria/createUpdate"
                },

                Listas = new ListaResource
                {
                    PorDondeVoyActual = "api/lista/iteracion",
                    GetByCodigo = "api/lista/index/codigo/{codigo}",
                    ResetPorDondeVoy = "api/lista/iteracion_reset",
                    InsertOrUpdate = "api/lista/createUpdate"
                },

                ListaDeArticulos = new ListaArticuloResource
                {
                    Eliminar = "api/lista/articulo_eliminar",
                    Insert = "api/lista/articulo_createUpdate"
                },

                Proveedores = new ProveedorResource
                {
                    GetFechaMaximaHistorial = "api/proveedor/historial",
                    InsertHistorico = "api/proveedor/historial_createUpdate",
                    GetOneByProveedor = "api/proveedor/index/proveedor/{proveedor}/nombre/{nombre}",
                    Insert = "api/proveedor/createUpdate",
                    Update = "api/proveedor/createUpdate"
                },

                Programacion = new ProgramacionResource
                {
                    Encendido = "api/programacion/encendido",
                    Apagado = "api/programacion/apagado"
                },

                Ventas = new VentaResource
                {
                    InsertVentaPendiente = "api/venta/createUpdate",
                    GetAllPendientes = "api/ventas/pendientes",
                    DeleteVentaPendiente = "api/venta/delete",
                }
            };
        }
    }
}

public class VentaResource
{
    public string InsertVentaPendiente { get; set; }

    public string GetAllPendientes { get; set; }

    public string DeleteVentaPendiente { get; set; }
}

public class CategoriaResource
{
    public string GetByCategoriaAndPadre { get; set; }

    public string GetByPadre { get; set; }

    public string Insert { get; internal set; }
}

public class ClienteResource
{
    public string GetDniTrackingLast { get; set; }

    public string ResetDniTracking { get; set; }

    public string GetByDni { get; set; }

    public string Insert { get; set; }

    public string Update { get; set; }

    public string InsertHueco { get; internal set; }
}

public class HuecoResource
{
    public string Exists { get; set; }

    public string Insert { get; set; }

    public string GetAll { get; set; }

    public string Delete { get; set; }
}

public class PuntoResource
{
    public string GetVentasNoActualizadas { get; set; }

    public string GetVentasRecetasPendientes { get; set; }

    public string GetSinRedencion { get; set; }

    public string Update { get; set; }

    public string GetLastOfYear { get; set; }

    public string GetByItemVenta { get; set; }

    public string Insert { get; internal set; }

    public string InsertActualizarVenta { get; internal set; }

    public string GetTimestampUltimaVenta { get; set; }

    public string ExistsByFechaGreatThanOrEqual { get; internal set; }

    public string GetPuntosByDni { get; internal set; }

    public string GetPuntosCanjeadosByDni { get; set; }

    public string Delete { get; set; }

    public string Exists { get; set; }
}

public class ConfiguracionResource
{
    public string GetValorByCampo { get; set; }

    public string UpdateValorByCampo { get; set; }

    public string PerteneceFarmazul { get; set; }

    public string GetAll { get; set; }

    public string ConexionLocal { get; set; }
}

public class EntregaResource
{
    public string GetByKey { get; set; }

    public string Insert { get; internal set; }
}

public class MedicamentoResource
{
    public string GetGreaterOrEqualByCodigoNacional { get; set; }

    public string Delete { get; set; }

    public string ResetSeguimientoSinStock { get; set; }

    public string ResetSeguimientoDondeVoy { get; set; }

    public string GetByCodNacional { get; internal set; }

    public string Insert { get; set; }

    public string Update { get; set; }
}

public class SinonimoResource
{
    public string IsEmpty { get; set; }

    public string Empty { get; set; }

    public string Insert { get; set; }
}

public class PedidoResource
{
    public string Ultimo { get; set; }

    public string GetByPedido { get; set; }

    public string Insert { get; internal set; }

    public string GetByLineaDePedido { get; set; }

    public string InsertLineaDePedido { get; set; }
}

public class FaltaResource
{
    public string Ultima { get; set; }

    public string GetByLineaDePedido { get; set; }

    public string InsertLineaDePedido { get; set; }
}

public class FamiliaResource
{
    public string GetByFamilia { get; set; }

    public string Insert { get; set; }

    public string GetPuntosByFamilia { get; set; }
}

public class EncargoResource
{
    public string Ultimo { get; set; }

    public string GetByEncargo { get; set; }

    public string Insert { get; set; }

    public string Update { get; set; }
}

public class ListaResource
{
    public string PorDondeVoyActual { get; set; }

    public string GetByCodigo { get; set; }

    public string ResetPorDondeVoy { get; set; }

    public string InsertOrUpdate { get; set; }
}

public class ListaArticuloResource
{
    public string Eliminar { get; set; }

    public string Insert { get; set; }
}

public class ProveedorResource
{
    public string GetFechaMaximaHistorial { get; set; }

    public string InsertHistorico { get; set; }

    public string GetOneByProveedor { get; set; }

    public string Insert { get; set; }

    public string Update { get; set; }
}

public class ProgramacionResource
{
    public string Encendido { get; set; }

    public string Apagado { get; set; }
}

public class Credential
{
    public string Username { get; set; }

    public string Password { get; set; }

    public string Token { get; set; }
}