namespace Sisfarma.Sincronizador.Fisiotes.Models
{
    using System;

    public partial class EntregaCliente
    {
        public long cod { get; set; }

        public long idventa { get; set; }

        public long idnlinea { get; set; }
  
        public string codigo { get; set; }

        public string descripcion { get; set; }

        public int cantidad { get; set; }

        public decimal precio { get; set; }

        public string tipo { get; set; }

        public int fecha { get; set; }

        public string dni { get; set; }

        public DateTime hora { get; set; }

        public string puesto { get; set; }

        public string trabajador { get; set; }

        public DateTime? fechaEntrega { get; set; }

        public float? pvp { get; set; }
    }
}
