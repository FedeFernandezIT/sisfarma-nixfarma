using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia.DTO
{
    public class LineaVenta
    {
        public int Farmaco { get; set; }

        public string Organismo { get; set; }

        public short Cantidad { get; set; }

        public int PVP { get; set; }

        public int Descuento { get; set; }

        public int Importe { get; set; }
    }
}
