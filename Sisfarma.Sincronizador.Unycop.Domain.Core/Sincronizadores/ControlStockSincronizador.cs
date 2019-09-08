﻿using System.Linq;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class ControlStockSincronizador : DC.ControlStockSincronizador
    {
        private const string FAMILIA_DEFAULT = "<Sin Clasificar>";
        private const string LABORATORIO_DEFAULT = "<Sin Laboratorio>";

        private const string TIPO_CLASIFICACION_DEFAULT = "Familia";
        private const string TIPO_CLASIFICACION_CATEGORIA = "Categoria";

        private string _clasificacion;
        private string _verCategorias;

        public ControlStockSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes)
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
            var farmacos = repository.GetWithStockByIdGreaterOrEqualAsDTO(_ultimoMedicamentoSincronizado);

            if (!farmacos.Any())
            {
                _sisfarma.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_CON_STOCK, "0");
                _ultimoMedicamentoSincronizado = "0";
                return;
            }

            foreach (var farmaco in farmacos)
            {
                Task.Delay(5).Wait();

                _cancellationToken.ThrowIfCancellationRequested();
                var medicamento = GenerarMedicamento(repository.GenerarFarmaco(farmaco));
                _sisfarma.Medicamentos.Sincronizar(medicamento);
                _ultimoMedicamentoSincronizado = medicamento.cod_nacional;
            }

            if (!_farmacia.Farmacos.AnyGreaterThatHasStock(_ultimoMedicamentoSincronizado))
            {
                _sisfarma.Configuraciones.Update(Configuracion.FIELD_POR_DONDE_VOY_CON_STOCK, "0");
                _ultimoMedicamentoSincronizado = "0";
            }
        }

        public Medicamento GenerarMedicamento(Farmaco farmaco)
        {
            var familia = farmaco.Familia?.Nombre ?? FAMILIA_DEFAULT;
            var superFamilia = farmaco.SuperFamilia?.Nombre ?? FAMILIA_DEFAULT;
            var familiaAux = _clasificacion == TIPO_CLASIFICACION_CATEGORIA ? familia : string.Empty;

            var categoria = farmaco.Categoria?.Nombre;
            if (_verCategorias == "si" && !string.IsNullOrWhiteSpace(categoria) && categoria.ToLower() != "sin categoria" && categoria.ToLower() != "sin categoría")
            {
                if (string.IsNullOrEmpty(superFamilia) || superFamilia == FAMILIA_DEFAULT)
                    superFamilia = categoria;
                else superFamilia = $"{superFamilia} ~~~~~~~~ {categoria}";
            }

            return new Medicamento
            {
                cod_barras = farmaco.CodigoBarras ?? "847000" + farmaco.Codigo.PadLeft(6, '0'),
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
                familiaAux = familiaAux,
                fechaCaducidad = farmaco.FechaCaducidad,
                fechaUltimaCompra = farmaco.FechaUltimaCompra,
                fechaUltimaVenta = farmaco.FechaUltimaVenta,
                baja = farmaco.Baja,
            };
        }
    }
}