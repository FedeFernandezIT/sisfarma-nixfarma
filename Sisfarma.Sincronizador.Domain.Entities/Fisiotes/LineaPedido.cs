namespace Sisfarma.Sincronizador.Domain.Entities.Fisiotes
{
    using System;

    public partial class LineaPedido
    {
        public int cantidadBonificada;

        public ulong id { get; set; }

        public DateTime? fechaPedido { get; set; }

        public long? idPedido { get; set; }

        public long? idLinea { get; set; }

        public long? cod_nacional { get; set; }

        public string descripcion { get; set; }

        public string familia { get; set; }

        public string superFamilia { get; set; }

        public long cantidad { get; set; }

        public decimal pvp { get; set; }

        public decimal puc { get; set; }

        public string proveedor { get; set; }

        public string cod_laboratorio { get; set; }

        public string laboratorio { get; set; }

        public string categoria { get; set; }

        public string subcategoria { get; set; }
    }
}