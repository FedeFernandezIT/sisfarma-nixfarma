using Sisfarma.Sincronizador.Config;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Farmatic.Repositories
{
    public abstract class FarmaticRepository
    {
        protected FarmaticContext _ctx;

        protected LocalConfig _config;

        public FarmaticRepository(FarmaticContext ctx)
        {
            _ctx = ctx ?? throw new ArgumentNullException(nameof(ctx));
        }

        public FarmaticRepository(LocalConfig config)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }
    }
}