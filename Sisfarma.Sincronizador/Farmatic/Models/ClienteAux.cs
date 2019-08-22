namespace Sisfarma.Sincronizador.Farmatic.Models
{
    using System;

    public partial class ClienteAux
    {
   
        public string IdCliente { get; set; }
   
        public string IdClienteFact { get; set; }

        public string LibreString1 { get; set; }

        public string LibreString2 { get; set; }

        public string LibreString3 { get; set; }

        public string LibreString4 { get; set; }

        public int? LibreInt1 { get; set; }

        public int? LibreInt2 { get; set; }

        public int? Boolean1 { get; set; }

        public int? Boolean2 { get; set; }

        public DateTime? FechaNac { get; set; }

        public DateTime? FechaAlta { get; set; }

        public int? fk_FormaPago_1 { get; set; }

        public string CuentaBancoPago { get; set; }

        public string CodigoCuentaCliente { get; set; }

        public int OpcClienteAux { get; set; }

        public string TipoPago { get; set; }

        public string Remesado { get; set; }

        public string IdMandato { get; set; }
    }
}
