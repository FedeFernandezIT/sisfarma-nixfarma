namespace Sisfarma.Sincronizador.Consejo.Models
{
    using System;

    public partial class Esperara
    {
        public string CODIGO { get; set; }
     
        public string NOMBRE { get; set; }

        public string PRESENTACION { get; set; }

        public string LABORATORIO { get; set; }

        public string GRUPOTERAPEUTICO { get; set; }

        public double? PRECIOLABORATORIO { get; set; }

        public double? PRECIOVENTA { get; set; }

        public string CONSERVACION { get; set; }

        public string CADUCIDAD { get; set; }

        public string DISPENSACION { get; set; }

        public string ESPECIALCONTROL { get; set; }

        public string HOSPITALARIAS { get; set; }

        public string APORTACION { get; set; }

        public string TLD { get; set; }

        public short? COMPOSICIONPOR { get; set; }

        public short? FORMAFARMACEUTICA { get; set; }

        public short? POSOLOGIA { get; set; }

        public int? UNIDADES { get; set; }

        public string TIPO { get; set; }

        public short? NACTIVOS { get; set; }

        public string PACTIVOS { get; set; }

        public string CANTIDADES { get; set; }

        public DateTime? FECHAALTA { get; set; }

        public DateTime? FECHABAJA { get; set; }

        public string EFG { get; set; }

        public int? ESPEUNIC { get; set; }

        public string CODESTA { get; set; }

        public int? ESPEFTEC { get; set; }

        public int? ESPEFPAC { get; set; }

        public int? ESPUNIE1 { get; set; }

        public DateTime? FECHAEXCSS { get; set; }

        public double? PRECIOFACTURA { get; set; }

        public DateTime? FECHAEXCFINANCIACION { get; set; }
    }
}
