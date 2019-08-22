namespace Sisfarma.Sincronizador.Farmatic.Models
{
    public partial class Cliente
    {
        public Cliente()
        {
        }
        
        public string IDCLIENTE { get; set; }

        public string TIPOTARIFA { get; set; }

        public string XCLIE_IDCLIENTEFACT { get; set; }

        public string GRUPOIVA { get; set; }

        public double? DESCUENTO { get; set; }

        public double? RIESGOMAX { get; set; }

        public string FIS_NIF { get; set; }

        public string FIS_NOMBRE { get; set; }

        public string FIS_DIRECCION { get; set; }

        public string FIS_CODPOSTAL { get; set; }

        public string FIS_POBLACION { get; set; }

        public string FIS_PROVINCIA { get; set; }

        public string FIS_TELEFONO { get; set; }

        public string FIS_FAX { get; set; }

        public string FIS_REGIMENFISCAL { get; set; }

        public string PER_NIF { get; set; }

        public string PER_NOMBRE { get; set; }

        public string PER_DIRECCION { get; set; }

        public string PER_CODPOSTAL { get; set; }

        public string PER_POBLACION { get; set; }

        public string PER_PROVINCIA { get; set; }

        public string PER_TELEFONO { get; set; }

        public string PER_FAX { get; set; }

        public short? XVEND_IDVENDEDOR { get; set; }

        public string OBSERVACIONES { get; set; }

        public string XTIPO_IDTIPO { get; set; }

        public string NASS { get; set; }

        public string NCOL { get; set; }

        public string XCUEN_IDCUENTA { get; set; }        
        
    }
}
