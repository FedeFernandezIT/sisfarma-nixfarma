using Sisfarma.Sincronizador.Consejo.Models;
using Sisfarma.Sincronizador.Consejo.Repositories;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Consejo
{
    public class ConsejoService
    {
        private readonly ConsejoContext _ctx;
        public EsperasRepository Esperas { get; private set; }
        public LaboratoriosRepository Laboratorios { get; set; }

        public ConsejoService()
        {
            _ctx = new ConsejoContext();
            Esperas = new EsperasRepository(_ctx);
            Laboratorios = new LaboratoriosRepository(_ctx);
        }
        
    }
}
