using System;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia.DTO
{
    public class Pedido
    {
        public short Id { get; set; }

        public int Proveedor { get; set; }

        public DateTime Fecha { get; set; }

        public int Farmaco { get; set; }
        
        public short CantInicial { get; set; }        
    }
}
