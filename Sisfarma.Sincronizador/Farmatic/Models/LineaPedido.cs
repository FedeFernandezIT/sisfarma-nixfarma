namespace Sisfarma.Sincronizador.Farmatic.Models
{
    public partial class LineaPedido
    {
        public int IdPedido { get; set; }

        public int IdLinea { get; set; }

        public string XArt_IdArticu { get; set; }

        public int Unidades { get; set; }

        public bool GestionFaltas { get; set; }

        public double Pvp { get; set; }

        public double Puc { get; set; }
        
    }
}
