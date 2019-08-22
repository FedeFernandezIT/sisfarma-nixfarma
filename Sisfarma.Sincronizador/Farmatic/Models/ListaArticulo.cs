namespace Sisfarma.Sincronizador.Farmatic.Models
{
    using System;

    public partial class ListaArticulo
    {
        public int IdLista { get; set; }

        public string Descripcion { get; set; }

        public DateTime? Fecha { get; set; }

        public int? NumElem { get; set; }

        public int? XList_IdFiltro { get; set; }

        public bool Tipo { get; set; }

    }
}
