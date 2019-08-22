namespace Sisfarma.Sincronizador.Consejo.Repositories
{
    public abstract class ConsejoRepository
    {
        protected ConsejoContext _ctx;

        public ConsejoRepository(ConsejoContext ctx)
        {
            _ctx = ctx;
        }
    }
}
