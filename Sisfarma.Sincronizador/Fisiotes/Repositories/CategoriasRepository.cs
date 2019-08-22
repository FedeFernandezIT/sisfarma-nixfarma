using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Fisiotes.Models;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Fisiotes.Repositories
{
    public class CategoriasRepository : FisiotesRepository
    {
        public CategoriasRepository(FisiotesContext ctx) : base(ctx)
        {
        }

        public CategoriasRepository(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        {
        }

        public Categoria GetByCategoriaAndPadreOrDefault(string categoria, string padre)
        {
            try
            {
                return _restClient
                    .Resource(_config.Categorias.GetByCategoriaAndPadre
                        .Replace("{categoria}", $"{categoria}")
                        .Replace("{padre}", $"{padre}"))
                    .SendGet<Categoria>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public bool Exists(string categoria, string padre)
        {
            return GetByCategoriaAndPadreOrDefault(categoria, padre) != null;
        }

        public Categoria GetByPadreOrDefault(string padre)
        {
            try
            {
                return _restClient
                    .Resource(_config.Categorias.GetByPadre                        
                        .Replace("{padre}", $"{padre}"))
                    .SendGet<Categoria>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public void Insert(Categoria cc)
        {
            var categoria = new
            {
                categoria = cc.categoria,
                padre = cc.padre,
                prestashopPadreId = cc.prestashopPadreId
            };

            _restClient
                .Resource(_config.Categorias.Insert)
                .SendPost(new
                {
                    categorias = new[] { categoria }
                });
        }

        #region SQL Methods

        public Categoria GetByCategoriaAndPadreSql(string categoria, string padre)
        {
            var sql = @"select * from ps_categorias where categoria = @categoria AND padre = @padre";
            return _ctx.Database.SqlQuery<Categoria>(sql,
                new SqlParameter("categoria", categoria),
                new SqlParameter("padre", padre))
                .FirstOrDefault();
        }

        public Categoria GetByPadreSql(string padre)
        {
            var sql = @"select * from ps_categorias where padre = @padre";
            return _ctx.Database.SqlQuery<Categoria>(sql,
                new SqlParameter("padre", padre))
                .FirstOrDefault();
        }

        public void Insert(string categoria, string padre, int? prestaShop)
        {
            var sql = @"INSERT IGNORE INTO ps_categorias (categoria, padre, prestashopPadreId) VALUES(" +
                    @"@categoria, @padre, @prestaShop)";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("categoria", categoria),
                new SqlParameter("padre", padre),
                new SqlParameter("prestaShop", prestaShop));
        }

        #endregion
    }
}