using System;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia.DTO
{
    public class Encargo
    {
        public int Id { get; set; }

        public string Farmaco { get; set; }

        public long Cliente { get; set; }

        public string Vendedor { get; set; }

        public DateTime FechaHora { get; set; }

        public DateTime FechaHoraEntrega { get; set; }

        public long Cantidad { get; set; }

        public string Observaciones { get; set; }

        public string Empresa { get; set; }

        public int Almacen { get; set; }

        public int Linea { get; set; }
    }
}