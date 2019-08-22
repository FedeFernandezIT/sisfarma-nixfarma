namespace Sisfarma.Sincronizador.Fisiotes.Models
{
    public partial class Categoria
    {
        public ulong id { get; set; }
        
        public string categoria { get; set; }

        public string padre { get; set; }

        public int? prestashopPadreId { get; set; }

        public int? prestashopId { get; set; }

        public bool? cargadoPS { get; set; }

        public bool? activoPrestashop { get; set; }

        public bool? activoPadrePrestashop { get; set; }
    }
}
