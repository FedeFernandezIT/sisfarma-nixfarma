using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Consejo
{
    public class ConsejoContext : DbContext
    {
        public ConsejoContext()
            : base("ConsejoContext")
        {
        }
    }
}
