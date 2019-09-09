using System;
using System.Collections;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Domain.Entities.Farmacia
{
    public class Lista
    {
        public long Id { get; set; }

        public string Descripcion { get; set; }

        public DateTime? Fecha { get; set; }

        public long NumElem { get; set; }

        public int? XList_IdFiltro { get; set; }

        public bool Tipo { get; set; }

        public ICollection<ListaDetalle> Farmacos { get; set; }

        public Lista() => Farmacos = new HashSet<ListaDetalle>();
    }

    public class ListaDetalle
    {
        public int Id { get; set; }

        public long ListaId { get; set; }

        public string FarmacoId { get; set; }
    }
}