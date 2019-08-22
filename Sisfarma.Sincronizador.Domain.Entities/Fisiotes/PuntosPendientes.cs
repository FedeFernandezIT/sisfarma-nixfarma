namespace Sisfarma.Sincronizador.Domain.Entities.Fisiotes
{
    using System;

    public partial class PuntosPendientes
    {
        public long Codigo { get; set; }

        public long VentaId { get; set; }

        public long LineaNumero { get; set; }

        public string CodigoBarra { get; set; }
     
        public string CodigoNacional { get; set; }

        public string Descripcion { get; set; }

        public string Familia { get; set; }

        public int Cantidad { get; set; }

        public decimal Precio { get; set; }

        public decimal Pago { get; set; }

        public string TipoPago { get; set; }

        public int Fecha { get; set; }

        public string DNI { get; set; }

        public string Cargado { get; set; }
        
        public DateTime hora { get; set; }

        public string cruzados { get; set; }
        
        public string Puesto { get; set; }

        public string Trabajador { get; set; }

        public string LaboratorioCodigo { get; set; }

        public string Laboratorio { get; set; }

        public string Proveedor { get; set; }

        public string Receta { get; set; }

        public DateTime? FechaVenta { get; set; }

        public string SuperFamilia { get; set; }

        public decimal PVP { get; set; }

        public decimal PUC { get; set; }

        public string Categoria { get; set; }

        public string Subcategoria { get; set; }

        public string Sistema { get; set; }

        public float? puntos { get; set; }

        public decimal LineaDescuento { get; set; }

        public decimal VentaDescuento { get; set; }

        public float? redencion { get; set; }

        public int? actualizado { get; set; }

        public string ubicacion { get; set; }

        public string recetaPendiente { get; set; }

        public long? TicketNumero { get; set; }
        public string Serie { get; set; }

        public string SuperFamiliaAux { get; set; }
        public string FamiliaAux { get; set; }
        public int CambioClasificacion { get; set; }
    }
}
