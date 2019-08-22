namespace Sisfarma.Sincronizador.Farmatic.Models
{
    using System;

    public partial class LineaRecepcion
    {
        public int IdRecepcion { get; set; }

        public int IdNLinea { get; set; }

        public string XArt_IdArticu { get; set; }

        public int Pedidas { get; set; }

        public int Recibidas { get; set; }

        public double ImportePvp { get; set; }

        public double ImportePuc { get; set; }

        public double Importe { get; set; }

        public double Descuento1 { get; set; }

        public double Descuento2 { get; set; }

        public double Descuento3 { get; set; }

        public double Descuento4 { get; set; }

        public bool DtoManual { get; set; }

        public DateTime FechCaducidad { get; set; }

        public int UPedir { get; set; }

        public int UDevolver { get; set; }

        public int UBonificar { get; set; }

        public bool Modificada { get; set; }

        public bool Baja { get; set; }

        public int? XPed_IdPedido { get; set; }

        public string PvpModificado { get; set; }

        public string PucModificado { get; set; }

        public string descuentofijo { get; set; }

        public double? palbaran { get; set; }

        public int? Marca { get; set; }

        public float? DFijo { get; set; }

        public decimal? DtoBonif { get; set; }
        
    }
}
