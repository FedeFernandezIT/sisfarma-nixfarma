namespace Sisfarma.Sincronizador.Farmatic.Models
{
    using System;

    public partial class Encargo
    {
        public string XArt_IdArticu { get; set; }
        
        public string XCli_IdCliente { get; set; }

        public int IdContador { get; set; }

        public DateTime IdFecha { get; set; }

        public int? Vendedor { get; set; }

        public int Unidades { get; set; }

        public DateTime? FechaEntrega { get; set; }

        public string Observaciones { get; set; }

        public int? Opciones { get; set; }

        public double? EntregadoCta { get; set; }

        public short? Estado { get; set; }

        public string Situacion { get; set; }

        public DateTime? FechaRecepcion { get; set; }

        public int? DiasRecogida { get; set; }
        
    }
}
