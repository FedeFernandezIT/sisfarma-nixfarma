namespace Sisfarma.Sincronizador.Farmatic.Models
{
    public partial class ItemListaArticulo
    {
        public int XItem_IdLista { get; set; }
     
        public string XItem_IdArticu { get; set; }

        public virtual ListaArticulo ListaArticu { get; set; }
    }
}
