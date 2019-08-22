namespace Sisfarma.Sincronizador.Farmatic.Models
{
    public partial class LineaVentaRedencion
    {
        public int IdVenta { get; set; }

        public int IdNLinea { get; set; }

        public string Codigo { get; set; }

        public decimal Redencion { get; set; }

        public decimal TotalRedencion { get; set; }
    }
}
