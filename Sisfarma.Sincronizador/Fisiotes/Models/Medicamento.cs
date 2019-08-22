namespace Sisfarma.Sincronizador.Fisiotes.Models
{
    using System;

    public partial class Medicamento
    {
        public string cod_barras { get; set; }

        public string cod_nacional { get; set; }

        public string nombre { get; set; }

        public string familia { get; set; }

        public string superFamilia { get; set; }

        public float precio { get; set; }

        public string descripcion { get; set; }

        public string laboratorio { get; set; }

        public string nombre_laboratorio { get; set; }

        public string proveedor { get; set; }

        public float? pvpSinIva { get; set; }

        public int? iva { get; set; }

        public int? stock { get; set; }

        public float? puc { get; set; }

        public int? stockMinimo { get; set; }

        public int? stockMaximo { get; set; }

        public string categoria { get; set; }

        public string subcategoria { get; set; }

        public bool? web { get; set; }

        public string ubicacion { get; set; }

        public string presentacion { get; set; }

        public string descripcionTienda { get; set; }

        public int? prestashopIdPS { get; set; }

        public bool? cargadoPS { get; set; }

        public DateTime? fechaCargadoPS { get; set; }

        public bool? activoPrestashop { get; set; }

        public bool? actualizadoPS { get; set; }

        public bool? eliminado { get; set; }

        public DateTime? fechaEliminado { get; set; }

        public string familiaAux { get; set; }

        public DateTime? fechaCaducidad { get; set; }

        public bool? porDondeVoy { get; set; }

        public bool? porDondeVoySinStock { get; set; }

        public DateTime? fechaUltimaCompra { get; set; }

        public DateTime? fechaUltimaVenta { get; set; }

        public bool? baja { get; set; }
    }
}
