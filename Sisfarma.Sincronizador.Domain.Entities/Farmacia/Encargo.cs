using System;

namespace Sisfarma.Sincronizador.Domain.Entities.Farmacia
{    
    public class Encargo
    {
        public long Id { get; set; }

        public Farmaco Farmaco { get; set; }

        public Cliente Cliente { get; set; }

        public Vendedor Vendedor { get; set; }

        public int Cantidad { get; set; }

        public DateTime Fecha { get; set; }

        public DateTime? FechaEntrega { get; set; }
        
        public string Observaciones { get; set; }
    }
}
