namespace Sisfarma.Sincronizador.Farmatic.Models
{
    using System;

    public partial class Pedido
    {
        public int IdPedido { get; set; }

        public string XProv_IdProveedor { get; set; }

        public bool Modem { get; set; }

        public int? NLineas { get; set; }

        public double? ImportePvp { get; set; }

        public double? ImportePuc { get; set; }

        public DateTime? Fecha { get; set; }

        public DateTime? Hora { get; set; }

        public string Representante { get; set; }

        public DateTime? FecVisita { get; set; }

        public string TelfContacto { get; set; }

        public DateTime? FecProxVisita { get; set; }

        public int? Tipo { get; set; }

    }
}
