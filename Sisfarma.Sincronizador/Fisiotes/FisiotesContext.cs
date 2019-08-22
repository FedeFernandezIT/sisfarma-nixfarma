using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Fisiotes
{
    public class FisiotesContext : DbContext
    {
        public FisiotesContext() 
            : base("FisiotesContext")
        {
        }
    }
}
