using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class MedicamentosExternalService : FisiotesExternalService, IMedicamentosExternalService
    {        
        public MedicamentosExternalService(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        { }

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

        public void Sincronizar(Medicamento medicamento)
        {
            throw new NotImplementedException();
        }
    }
}