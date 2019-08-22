using System;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia.DTO
{
    public class Encargo
    {
        public int Id { get; set; }

        public int? Farmaco { get; set; }

        public int? Cliente { get; set; }

        public int? Vendedor { get; set; }

        public DateTime? FechaHora { get; set; }

        public DateTime? FechaHoraEntrega { get; set; }

        public short Cantidad { get; set; }

        public string Observaciones { get; set; }
    }
}
