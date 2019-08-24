using System;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia.DTO
{
    public class ProveedorHistorico
    {
        public int Farmaco { get; set; }

        public int? Proveedor { get; set; }

        public DateTime? Fecha { get; set; }

        public int PVAlbaran { get; set; }

        public int PC { get; set; }
    }
}
