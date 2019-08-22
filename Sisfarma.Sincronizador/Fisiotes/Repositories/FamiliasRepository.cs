using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Fisiotes.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Fisiotes.Repositories
{
    public class FamiliasRepository : FisiotesRepository
    {
        public FamiliasRepository(FisiotesContext ctx) : base(ctx)
        {
        }

        public FamiliasRepository(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        {
        }

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

        #region SQL Methods
        public Familia GetByFamiliaSql(string familia)
        {
            var sql = @"select * from familia where familia = @familia";
            return _ctx.Database.SqlQuery<Familia>(sql,
                new SqlParameter("familia", familia))
                .FirstOrDefault();
        }

        public void InsertSql(string familia)
        {
            var sql = @"INSERT IGNORE INTO familia (familia) VALUES(@familia)";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("familia", familia));
        }        

        #endregion
    }
}