using System;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class ControlStockFechaSalidaSincronizador : DC.ControlStockFechaSalidaSincronizador
    {
        private const string FAMILIA_DEFAULT = "<Sin Clasificar>";
        private const string LABORATORIO_DEFAULT = "<Sin Laboratorio>";

        private const string TIPO_CLASIFICACION_DEFAULT = "Familia";
        private const string TIPO_CLASIFICACION_CATEGORIA = "Categoria";

        private string _clasificacion;
        private string _verCategorias;

        public ControlStockFechaSalidaSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes)
            : base(farmacia, fisiotes)
        { }

        public override void LoadConfiguration()
        {
            base.LoadConfiguration();
            _clasificacion = !string.IsNullOrWhiteSpace(ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION])
                ? ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION]
                : TIPO_CLASIFICACION_DEFAULT;
            _verCategorias = ConfiguracionPredefinida[Configuracion.FIELD_VER_CATEGORIAS];
        }

        public override void PreSincronizacion()
        {
            base.PreSincronizacion();
        }

        public override void Process()
        {
            var repository = _farmacia.Farmacos as FarmacoRespository;
            var farmacos = repository.GetAllByFechaUltimaSalidaGreaterOrEqualAsDTO(_ultimoFechaActualizacionStockSincronizado);

            foreach (var farmaco in farmacos)
            {
                Task.Delay(5).Wait();

                _cancellationToken.ThrowIfCancellationRequested();
                var medicamento = GenerarMedicamento(repository.GenerarFarmaco(farmaco));
                _sisfarma.Medicamentos.Sincronizar(medicamento);

                _ultimoFechaActualizacionStockSincronizado = medicamento.fechaUltimaVenta ?? DateTime.Now;
            }
        }

        public Medicamento GenerarMedicamento(Farmaco farmaco)
        {
            var familia = !string.IsNullOrWhiteSpace(farmaco.Familia?.Nombre) ? farmaco.Familia.Nombre : FAMILIA_DEFAULT;
            var superFamilia = !string.IsNullOrWhiteSpace(farmaco.SuperFamilia?.Nombre) ? farmaco.SuperFamilia.Nombre : FAMILIA_DEFAULT;

            var categoria = farmaco.Categoria?.Nombre;
            if (_verCategorias == "si" && !string.IsNullOrWhiteSpace(categoria) && categoria.ToLower() != "sin categoria" && categoria.ToLower() != "sin categoría")
            {
                if (string.IsNullOrEmpty(superFamilia) || superFamilia == FAMILIA_DEFAULT)
                    superFamilia = categoria;
                else superFamilia = $"{superFamilia} ~~~~~~~~ {categoria}";
            }

            return new Medicamento
            {
                cod_barras = !string.IsNullOrEmpty(farmaco.CodigoBarras) ? farmaco.CodigoBarras : "847000" + farmaco.Codigo.PadLeft(6, '0'),
                cod_nacional = farmaco.Codigo,
                nombre = farmaco.Denominacion,
                familia = familia,
                superFamilia = superFamilia,
                precio = farmaco.Precio,
                descripcion = farmaco.Denominacion,
                laboratorio = farmaco.Laboratorio?.Codigo ?? "0",
                nombre_laboratorio = farmaco.Laboratorio?.Nombre ?? LABORATORIO_DEFAULT,
                proveedor = farmaco.Proveedor?.Nombre ?? string.Empty,
                pvpSinIva = farmaco.PrecioSinIva(),
                iva = (int)farmaco.Iva,
                stock = farmaco.Stock,
                puc = farmaco.PrecioCoste,
                stockMinimo = farmaco.StockMinimo,
                stockMaximo = farmaco.StockMaximo,
                categoria = farmaco.Categoria?.Nombre ?? string.Empty,
                ubicacion = farmaco.Ubicacion ?? string.Empty,
                presentacion = string.Empty,
                descripcionTienda = string.Empty,
                activoPrestashop = !farmaco.Baja,
                fechaCaducidad = farmaco.FechaCaducidad,
                fechaUltimaCompra = farmaco.FechaUltimaCompra,
                fechaUltimaVenta = farmaco.FechaUltimaVenta,
                baja = farmaco.Baja,
            };
        }
    }
}