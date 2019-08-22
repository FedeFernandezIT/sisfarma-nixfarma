namespace Sisfarma.Sincronizador.Fisiotes.Models
{
    using System;

    public partial class Encargo
    {        
        public ulong id { get; set; }

        public long? idEncargo { get; set; }

        public long? idLinea { get; set; }
     
        public string cod_nacional { get; set; }

        public string nombre { get; set; }

        public string superFamilia { get; set; }

        public string familia { get; set; }

        public string cod_laboratorio { get; set; }

        public string laboratorio { get; set; }

        public string proveedor { get; set; }

        public float? pvp { get; set; }

        public float? puc { get; set; }

        public string dni { get; set; }

        public DateTime? fecha { get; set; }

        public string trabajador { get; set; }

        public int? unidades { get; set; }

        public DateTime? fechaEntrega { get; set; }

        public string observaciones { get; set; }

        public string categoria { get; set; }

        public string subcategoria { get; set; }

        public string superFamiliaAux { get; set; }

        public string familiaAux { get; set; }

        public bool? cambioClasificacion { get; set; }

        public string empresa_codigo { get; set; }

        public int? almacen_codigo { get; set; }
    }
}
