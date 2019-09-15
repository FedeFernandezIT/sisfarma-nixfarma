using System;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia.DTO
{
    public class Recepcion
    {
        public DateTime Fecha { get; set; }

        public int? Albaran { get; set; }

        public long? Proveedor { get; set; }

        public string Farmaco { get; set; }

        public int PVP { get; set; }

        public int PC { get; set; }

        public int PVAlbaran { get; set; }

        public int PCTotal { get; set; }

        public long Recibido { get; set; }

        public int Bonificado { get; set; }

        public int Devuelto { get; set; }

        public string Empresa { get; set; }

        public long Pedido { get; set; }

        public decimal ImportePvp { get; set; }

        public decimal ImportePuc { get; set; }

        public long Linea { get; set; }
    }

    public class RecepcionTotales
    {
        public int Lineas { get; set; }

        public decimal PVP { get; set; }

        public decimal PUC { get; set; }
    }
}