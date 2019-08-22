namespace Sisfarma.Sincronizador.Farmatic.Models
{
    public partial class Proveedor
    {
        public string IDPROVEEDOR { get; set; }

        public double? DESCUENTO { get; set; }

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

        public string OBSERVACIONES { get; set; }

        public string GRUPOIVA { get; set; }

        public string XCUEN_IDCUENTA { get; set; }

        public short? XPROT_IDPROTOCOLO { get; set; }

        public short? XPROT_IDPROTOCOLO2 { get; set; }

        public short? XPROT_IDPROTOCOLO3 { get; set; }

        public short? XPROT_IDPROTOCOLO4 { get; set; }

        public short? IDCONDICION { get; set; }

        public int? cargodto { get; set; }

        public string coniva { get; set; }

        public int? fk_FormaPago_1 { get; set; }

        public string CuentaBancoPago { get; set; }

        public string DELEG_NOMBRE { get; set; }

        public string DELEG_TLF { get; set; }

        public string Web { get; set; }

        public int? OpcProveedor { get; set; }

        public string TipoAlmLab { get; set; }

        public string Sigla { get; set; }

        public int? DistriAlbElectronico { get; set; }
        
    }
}
