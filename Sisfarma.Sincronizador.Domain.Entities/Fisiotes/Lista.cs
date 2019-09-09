namespace Sisfarma.Sincronizador.Domain.Entities.Fisiotes
{
    public partial class Lista
    {
        public long cod { get; set; }

        public string lista { get; set; }

        public int? numArticulos { get; set; }

        public int? porDondeVoy { get; set; }
    }
}