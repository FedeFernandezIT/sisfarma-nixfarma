namespace Sisfarma.Sincronizador.Fisiotes.Models
{
    using System;

    public partial class PuntosPendientes
    {
        public long cod { get; set; }

        public long idventa { get; set; }

        public long idnlinea { get; set; }

        public string cod_barras { get; set; }
     
        public string cod_nacional { get; set; }

        public string descripcion { get; set; }

        public string familia { get; set; }

        public int cantidad { get; set; }

        public decimal precio { get; set; }

        public string tipoPago { get; set; }

        public int fecha { get; set; }

        public string dni { get; set; }

        public string cargado { get; set; }
        
        public DateTime hora { get; set; }

        public string cruzados { get; set; }
        
        public string puesto { get; set; }

        public string trabajador { get; set; }

        public string cod_laboratorio { get; set; }

        public string laboratorio { get; set; }

        public string proveedor { get; set; }

        public string receta { get; set; }

        public DateTime? fechaVenta { get; set; }

        public string superFamilia { get; set; }

        public float? pvp { get; set; }

        public float? puc { get; set; }

        public string categoria { get; set; }

        public string subcategoria { get; set; }

        public string sistema { get; set; }

        public float? puntos { get; set; }

        public float? dtoLinea { get; set; }

        public float? dtoVenta { get; set; }

        public float? redencion { get; set; }

        public int? actualizado { get; set; }

        public string ubicacion { get; set; }

        public string recetaPendiente { get; set; }
    }
}
