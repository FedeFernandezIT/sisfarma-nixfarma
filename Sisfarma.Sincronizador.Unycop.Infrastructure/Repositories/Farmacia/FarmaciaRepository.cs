using Sisfarma.Sincronizador.Core.Config;
using System;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia
{
    public abstract class FarmaciaRepository
    {        
        protected LocalConfig _config;
        
        public FarmaciaRepository(LocalConfig config)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }

        public FarmaciaRepository() { }
    }
}