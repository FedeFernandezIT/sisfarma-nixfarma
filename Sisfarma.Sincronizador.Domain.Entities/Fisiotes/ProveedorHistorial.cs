using System;

namespace Sisfarma.Sincronizador.Domain.Entities.Fisiotes
{
    public class ProveedorHistorial
    {
        public int id { get; set; }

        public string idProveedor { get; set; }

        public string cod_nacional { get; set; }

        public DateTime? fecha { get; set; }

        public decimal? puc { get; set; }
    }
}
