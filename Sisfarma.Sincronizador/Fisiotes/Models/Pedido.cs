namespace Sisfarma.Sincronizador.Fisiotes.Models
{
    using System;

    public partial class Pedido
    {
        public ulong id { get; set; }

        public long? idPedido { get; set; }

        public DateTime? fechaPedido { get; set; }

        public DateTime? hora { get; set; }

        public int? numLineas { get; set; }

        public float? importePvp { get; set; }

        public float? importePuc { get; set; }
        
        public string idProveedor { get; set; }

        public string proveedor { get; set; }

        public string trabajador { get; set; }

        public string sistema { get; set; }
    }
}
