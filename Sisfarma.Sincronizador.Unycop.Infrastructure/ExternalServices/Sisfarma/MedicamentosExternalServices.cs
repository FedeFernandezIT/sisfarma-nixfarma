using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes;
using System;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.ExternalServices.Sisfarma
{
    public class MedicamentosExternalServices : FisiotesExternalService, IMedicamentosExternalService
    {
        public MedicamentosExternalServices(IRestClient restClient, FisiotesConfig config) 
            : base(restClient, config)
        { }

        public void DeleteByCodigoNacional(string codigo)
        {
            _restClient
                .Resource(_config.Medicamentos.Delete)
                .SendPut(new
                {
                    id = codigo
                });
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

        public Medicamento GetOneOrDefaultByCodNacional(string codNacional)
        {
            throw new NotImplementedException();
        }

        public void Insert(Medicamento mm)
        {
            throw new NotImplementedException();
        }

        public void Insert(string codigoBarras, string codNacional, string nombre, string superFamilia, string familia, float precio, string descripcion, string laboratorio, string nombreLaboratorio, string proveedor, float pvpSinIva, int iva, int stock, float puc, int stockMinimo, int stockMaximo, string presentacion, string descripcionTienda, bool activo, DateTime? caducidad, DateTime? ultimaCompra, DateTime? ultimaVenta, bool baja)
        {
            throw new NotImplementedException();
        }

        public void ResetPorDondeVoy()
        {
            throw new NotImplementedException();
        }

        public void ResetPorDondeVoySinStock()
        {
            throw new NotImplementedException();
        }

        public void Sincronizar(Medicamento mm)
        {
            var medicamento = new[] { new
                {
                    actualizadoPS = 1,
                    cod_barras = mm.cod_barras.Strip(),
                    cod_nacional = mm.cod_nacional,
                    nombre = mm.nombre.Strip(),
                    familia = mm.familia.Strip(),                    
                    familiaAux = mm.familiaAux.Strip(),
                    precio = mm.precio,
                    descripcion = mm.descripcion.Strip(),
                    laboratorio = mm.laboratorio.Strip(),
                    nombre_laboratorio = mm.nombre_laboratorio.Strip(),
                    proveedor = mm.proveedor.Strip(),
                    pvpSinIva = mm.pvpSinIva,
                    iva = mm.iva,
                    stock = mm.stock,
                    puc = mm.puc,
                    stockMinimo = mm.stockMinimo,
                    stockMaximo = mm.stockMaximo,
                    categoria = mm.categoria.Strip(),
                    subcategoria = mm.subcategoria.Strip(),
                    web = mm.web.ToInteger(),
                    ubicacion = mm.ubicacion.Strip(),
                    presentacion = mm.presentacion,
                    descripcionTienda = mm.descripcionTienda,
                    activoPrestashop = mm.activoPrestashop.ToInteger(),
                    fechaCaducidad = mm.fechaCaducidad?.ToDateInteger("yyyyMM") ?? 0,
                    fechaUltimaCompra = mm.fechaUltimaCompra.ToIsoString(),
                    fechaUltimaVenta = mm.fechaUltimaVenta.ToIsoString(),
                    baja = mm.baja.ToInteger()
                }};

            _restClient.
                Resource(_config.Medicamentos.Insert)
                .SendPost(new { bulk = medicamento });

        }

        public void Update(Medicamento mm, bool withSqlExtra = false)
        {
            throw new NotImplementedException();
        }

        public void Update(string codigoBarras, string nombre, string superFamilia, string familia, float precio, string descripcion, string laboratorio, string nombreLaboratorio, string proveedor, int iva, float pvpSinIva, int stock, float puc, int stockMinimo, int stockMaximo, string presentacion, string descripcionTienda, bool activo, DateTime? caducidad, DateTime? ultimaCompra, DateTime? ultimaVenta, bool baja, string codNacional, bool withSqlExtra = false)
        {
            throw new NotImplementedException();
        }
    }
}
