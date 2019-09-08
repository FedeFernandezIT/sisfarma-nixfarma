namespace Sisfarma.Sincronizador.Domain.Entities.Fisiotes
{
    using System;

    public partial class Falta
    {
        public long cod { get; set; }

        public long idPedido { get; set; }

        public long? idLinea { get; set; }

        public string cod_nacional { get; set; }

        public string descripcion { get; set; }

        public string familia { get; set; }

        public string superFamilia { get; set; }

        public long cantidadPedida { get; set; }

        public DateTime? fechaFalta { get; set; }

        public string cod_laboratorio { get; set; }

        public string laboratorio { get; set; }

        public string proveedor { get; set; }

        public DateTime fechaPedido { get; set; }

        public decimal pvp { get; set; }

        public decimal puc { get; set; }

        public string sistema { get; set; }

        public string categoria { get; set; }

        public string subcategoria { get; set; }

        public string superFamiliaAux { get; set; }

        public string familiaAux { get; set; }

        public bool? cambioClasificacion { get; set; }
    }
}