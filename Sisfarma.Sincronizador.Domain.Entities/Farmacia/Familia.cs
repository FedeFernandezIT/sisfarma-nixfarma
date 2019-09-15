using System.Collections.Generic;
using System.Linq;

namespace Sisfarma.Sincronizador.Domain.Entities.Farmacia
{
    public class Familia
    {
        public long Id { get; set; }

        public string Codigo { get; set; }

        public string Nombre { get; set; }

        public string Padre { get; set; }

        public ICollection<Categoria> Categorias { get; set; }

        public Familia() => Categorias = new HashSet<Categoria>();

        public bool HasCategorias() => Categorias != null && Categorias.Any();
    }
}