using System;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class ControlStockFechaEntradaSincronizador : DC.ControlStockFechaEntradaSincronizador
    {
        private const string FAMILIA_DEFAULT = "<Sin Clasificar>";
        private const string LABORATORIO_DEFAULT = "<Sin Laboratorio>";

        private const string TIPO_CLASIFICACION_DEFAULT = "Familia";
        private const string TIPO_CLASIFICACION_CATEGORIA = "Categoria";

        private string _clasificacion;

        public ControlStockFechaEntradaSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        { }

        public override void LoadConfiguration()
        {
            base.LoadConfiguration();
            _clasificacion = !string.IsNullOrWhiteSpace(ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION])
                ? ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION]
                : TIPO_CLASIFICACION_DEFAULT;
        }

        public override void PreSincronizacion()
        {
            base.PreSincronizacion();
        }

        public override void Process()
        {
            var repository = _farmacia.Farmacos as FarmacoRespository;
            var farmacos = repository.GetAllByFechaUltimaEntradaGreaterOrEqualAsDTO(_ultimoFechaActualizacionStockSincronizado);

            foreach (var farmaco in farmacos)
            {
                Task.Delay(5).Wait();

                _cancellationToken.ThrowIfCancellationRequested();
                var medicamento = GenerarMedicamento(repository.GenerarFarmaco(farmaco));
                _sisfarma.Medicamentos.Sincronizar(medicamento);

                _ultimoFechaActualizacionStockSincronizado = medicamento.fechaUltimaCompra ?? DateTime.Now;
            }
        }

        public Medicamento GenerarMedicamento(Farmaco farmaco)
        {            
            var familia = farmaco.Familia?.Nombre ?? FAMILIA_DEFAULT;
            var familiaAux = _clasificacion == TIPO_CLASIFICACION_CATEGORIA ? familia : string.Empty;

            return new Medicamento
            {
                cod_barras = farmaco.CodigoBarras ?? "847000" + farmaco.Codigo.PadLeft(6, '0'),
                cod_nacional = farmaco.Id.ToString(),
                nombre = farmaco.Denominacion,
                familia = familia,
                precio = (float)farmaco.Precio,
                descripcion = farmaco.Denominacion,
                laboratorio = farmaco.Laboratorio?.Codigo ?? "0",
                nombre_laboratorio = farmaco.Laboratorio?.Nombre ?? LABORATORIO_DEFAULT,
                proveedor = farmaco.Proveedor?.Nombre ?? string.Empty,
                pvpSinIva = (float)farmaco.PrecioSinIva(),
                iva = (int)farmaco.Iva,
                stock = farmaco.Stock,
                puc = (float)farmaco.PrecioCoste,
                stockMinimo = farmaco.StockMinimo,
                stockMaximo = 0,
                categoria = farmaco.Categoria?.Nombre ?? string.Empty,
                subcategoria = farmaco.Subcategoria?.Nombre ?? string.Empty,
                web = farmaco.Web,
                ubicacion = farmaco.Ubicacion ?? string.Empty,
                presentacion = string.Empty,
                descripcionTienda = string.Empty,
                activoPrestashop = !farmaco.Baja,
                familiaAux = familiaAux,
                fechaCaducidad = farmaco.FechaCaducidad,
                fechaUltimaCompra = farmaco.FechaUltimaCompra,
                fechaUltimaVenta = farmaco.FechaUltimaVenta,
                baja = farmaco.Baja
            };

        }
    }
}
