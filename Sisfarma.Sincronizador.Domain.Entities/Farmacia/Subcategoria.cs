namespace Sisfarma.Sincronizador.Domain.Entities.Farmacia
{
    public class Subcategoria
    {
        public long Id { get; set; }

        public string Codigo { get; set; }

        public string Nombre { get; set; }

        public Categoria Categoria { get; set; }
    }
}
