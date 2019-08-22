namespace Sisfarma.Sincronizador.Farmatic.Models
{
    public partial class LineaVentaVirtual
    {
        public int IdVenta { get; set; }

        public int IdNLinea { get; set; }

        public string Codigo { get; set; }

        public string Descripcion { get; set; }

        public int Cantidad { get; set; }

        public double Pvp { get; set; }

        public string TipoAportacion { get; set; }

        public double ImporteBruto { get; set; }

        public double? DescuentoLinea { get; set; }

        public double? DescuentoOpera { get; set; }

        public double ImporteNeto { get; set; }

        public string TipoLinea { get; set; }

        public string RecetaPendiente { get; set; }

        public string Facturada { get; set; }

        public int? Factura { get; set; }

        public int? Libre1 { get; set; }

        public double? Libre2 { get; set; }

        public string Libre3 { get; set; }

    }
}
