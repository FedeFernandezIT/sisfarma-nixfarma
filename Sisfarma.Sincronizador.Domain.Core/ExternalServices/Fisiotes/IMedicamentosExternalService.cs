using System;
using System.Collections.Generic;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IMedicamentosExternalService : IMedicamentosExternalServiceNew
    {
        void DeleteByCodigoNacional(string codigo);
        IEnumerable<Medicamento> GetGreaterOrEqualCodigosNacionales(string codigo);
        Medicamento GetOneOrDefaultByCodNacional(string codNacional);
        void Insert(Medicamento mm);
        void Insert(string codigoBarras, string codNacional, string nombre, string superFamilia, string familia, float precio, string descripcion, string laboratorio, string nombreLaboratorio, string proveedor, float pvpSinIva, int iva, int stock, float puc, int stockMinimo, int stockMaximo, string presentacion, string descripcionTienda, bool activo, DateTime? caducidad, DateTime? ultimaCompra, DateTime? ultimaVenta, bool baja);
        void ResetPorDondeVoy();
        void ResetPorDondeVoySinStock();
        void Update(Medicamento mm, bool withSqlExtra = false);
        void Update(string codigoBarras, string nombre, string superFamilia, string familia, float precio, string descripcion, string laboratorio, string nombreLaboratorio, string proveedor, int iva, float pvpSinIva, int stock, float puc, int stockMinimo, int stockMaximo, string presentacion, string descripcionTienda, bool activo, DateTime? caducidad, DateTime? ultimaCompra, DateTime? ultimaVenta, bool baja, string codNacional, bool withSqlExtra = false);
    }

    public interface IMedicamentosExternalServiceNew
    {
        void Sincronizar(IEnumerable<Medicamento> medicamentos);
    }
}