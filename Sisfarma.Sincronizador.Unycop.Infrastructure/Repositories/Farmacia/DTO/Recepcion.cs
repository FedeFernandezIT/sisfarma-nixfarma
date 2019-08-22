using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia.DTO
{
    public class Recepcion
    {
        public DateTime? Fecha { get; set; }

        public int? Albaran { get; set; }

        public int? Proveedor { get; set; }

        public int Farmaco { get; set; }

        public int PVP { get; set; }

        public int PC { get; set; }

        public int PVAlbaran { get; set; }

        public int PCTotal { get; set; }

        public int Recibido { get; set; }

        public int Bonificado { get; set; }

        public int Devuelto { get; set; }
    }

    public class RecepcionTotales
    {
        public int Lineas { get; set; }

        public int PVP { get; set; }

        public int PUC { get; set; }
    }
}
