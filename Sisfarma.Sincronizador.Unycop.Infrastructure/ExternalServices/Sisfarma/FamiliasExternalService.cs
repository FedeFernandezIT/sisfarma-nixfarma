using Sisfarma.RestClient;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes;
using System;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.ExternalServices.Sisfarma
{
    public class FamiliasExternalService : FisiotesExternalService, IFamiliasExternalService
    {
        public FamiliasExternalService(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        { }

        public bool Exists(string familia)
        {
            throw new NotImplementedException();
        }

        public Familia GetByFamilia(string familia)
        {
            throw new NotImplementedException();
        }

        public decimal GetPuntosByFamiliaTipoVerificado(string familia)
        {
            throw new NotImplementedException();
        }

        public void Insert(Familia ff)
        {
            throw new NotImplementedException();
        }

        public void

            Sincronizar(string nombre, string tipo = null)
        {
            var familia = new
            {
                familia = nombre,
                tipo = tipo
            };

            _restClient
                .Resource(_config.Familias.Insert)
                .SendPost(new
                {
                    bulk = new[] { familia }
                });
        }
    }
}