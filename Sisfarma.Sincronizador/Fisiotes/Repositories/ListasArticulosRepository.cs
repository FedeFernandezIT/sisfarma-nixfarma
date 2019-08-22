using Sisfarma.RestClient;
using Sisfarma.Sincronizador.Fisiotes.Models;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Fisiotes.Repositories
{
    public class ListasArticulosRepository : FisiotesRepository
    {
        public ListasArticulosRepository(FisiotesContext ctx) : base(ctx)
        {
        }

        public ListasArticulosRepository(IRestClient restClient, FisiotesConfig config) :
            base(restClient, config)
        {
        }

        public void Delete(int codigo)
        {
            _restClient
                .Resource(_config.ListaDeArticulos.Eliminar)
                .SendPut(new { ids = new[] { codigo } });
        }

        public void Insert(List<ListaArticulo> items)
        {
            var articulos = items.Select(i => new
            {
                cod_lista = i.cod_lista,
                cod_articulo = i.cod_articulo
            });

            
            _restClient
                .Resource(_config.ListaDeArticulos.Insert)
                .SendPost(new
                {
                    bulk = articulos
                });
        }

        public void Insert(ListaArticulo la)
        {
            Insert(new List<ListaArticulo> { la });
        }


        public void Insert(int lista, int articulo)
        {
            var sql = @"INSERT IGNORE INTO listas_articulos (cod_lista,cod_articulo) VALUES (@lista, @articulo)";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("lista", lista),
                new SqlParameter("articulo", articulo));
        }
        
        #region SQL Methods

        public void DeleteSql(int codigo)
        {
            var sql = @"DELETE FROM listas_articulos WHERE cod_lista = @codigo";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("codigo", codigo));
        }


        public void InsertSql(List<ListaArticulo> items)
        {
            var sql = @"INSERT IGNORE INTO listas_articulos (cod_lista,cod_articulo) VALUES ";
            foreach (var item in items)
            {
                sql += $@"('{item.cod_lista}', '{item.cod_articulo}'),";
            }
            sql = sql.TrimEnd(',');
            _ctx.Database.ExecuteSqlCommand(sql);
        }

        #endregion


    }
}