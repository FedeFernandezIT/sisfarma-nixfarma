using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class FamiliasExternalService : FisiotesExternalService, IFamiliasExternalService
    {
        public FamiliasExternalService(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        { }

        public Familia GetByFamilia(string familia)
        {
            try
            {
                return _restClient
                    .Resource(_config.Familias.GetByFamilia.Replace("{familia}", familia))
                    .SendGet<Familia>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public bool Exists(string familia)
        {
            return GetByFamilia(familia) != null;
        }

        public void Insert(Familia ff)
        {
            var familia = new
            {
                familia = ff.familia,
                puntos = ff.puntos,
                nivel1 = ff.nivel1,
                nivel2 = ff.nivel2,
                nivel3 = ff.nivel3,
                nivel4 = ff.nivel4
            };

            _restClient
                .Resource(_config.Familias.Insert)
                .SendPost(new
                {
                    bulk = new[] { familia }
                });
        }

        public decimal GetPuntosByFamiliaTipoVerificado(string familia)
        {
            try
            {
                return _restClient
                    .Resource(_config.Familias.GetPuntosByFamilia.Replace("{familia}", familia))
                    .SendGet<decimal>();
            }
            catch (RestClientNotFoundException)
            {
                return 0m;
            }
        }

        public void Sincronizar(IEnumerable<Familia> familias)
        {
            throw new System.NotImplementedException();
        }
    }
}