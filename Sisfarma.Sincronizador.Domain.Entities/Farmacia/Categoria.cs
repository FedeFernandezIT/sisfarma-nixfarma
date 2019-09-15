using System.Collections.Generic;
using System.Linq;

namespace Sisfarma.Sincronizador.Domain.Entities.Farmacia
{
    public class Categoria
    {
        public long Id { get; set; }

        public string Codigo { get; set; }

        public string Nombre { get; set; }

        public Familia Familia { get; set; }

        public IEnumerable<string> Subcategorias { get; set; } = new List<string>();

        public bool HasSubcategorias() => Subcategorias != null && Subcategorias.Any();
    }
}
