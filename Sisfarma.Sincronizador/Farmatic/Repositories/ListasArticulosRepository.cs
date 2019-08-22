using Sisfarma.Sincronizador.Config;
using Sisfarma.Sincronizador.Farmatic.Models;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Farmatic.Repositories
{
    public class ListasArticulosRepository : FarmaticRepository
    {
        public ListasArticulosRepository(FarmaticContext ctx) : base(ctx)
        { }

        public ListasArticulosRepository(LocalConfig config) : base(config)
        { }

        public ListaArticulo GetOneOrDefault(int lista)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM ListaArticu WHERE fecha >= DATEADD(dd, 0, DATEDIFF(dd, 0, GETDATE())) AND idLista = @lista";
                return db.Database.SqlQuery<ListaArticulo>(sql,
                    new SqlParameter("lista", lista))
                    .FirstOrDefault();
            }
        }

        public ArticuloWithIva GetArticuloWithIva(int lista, string articulo)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"select a.*, t.Piva AS iva from articu a INNER JOIN Tablaiva t ON t.IdTipoArt = a.XGrup_IdGrupoIva AND t.IdTipoPro = '05' " +
                    @"INNER JOIN ItemListaArticu li ON li.XItem_IdArticu = a.idArticu AND li.XItem_IdLista = @lista " +
                    @"WHERE a.idArticu = @articulo";
                return db.Database.SqlQuery<ArticuloWithIva>(sql,
                        new SqlParameter("lista", lista),
                        new SqlParameter("articulo", articulo))
                        .FirstOrDefault();
            }
        }

        public IEnumerable<ListaArticulo> GetByIdGreaterThan(int lista)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM ListaArticu WHERE idLista > @lista";
                return db.Database.SqlQuery<ListaArticulo>(sql,
                    new SqlParameter("lista", lista))
                    .ToList();
            }
        }

        public List<ListaArticulo> GetByFechaExceptList(int lista)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM ListaArticu WHERE fecha >= DATEADD(dd, 0, DATEDIFF(dd, 0, GETDATE())) AND idLista <> @lista";
                return db.Database.SqlQuery<ListaArticulo>(sql,
                    new SqlParameter("lista", lista))
                    .ToList();
            }
        }

        public List<ItemListaArticulo> GetArticulosByLista(int lista)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM ItemListaArticu WHERE XItem_IdLista = @lista GROUP BY XItem_IdLista, XItem_IdArticu";
                return db.Database.SqlQuery<ItemListaArticulo>(sql,
                    new SqlParameter("lista", lista))
                    .ToList();
            }
        }

        public void Update(int lista)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"UPDATE ListaArticu SET fecha = DATEADD(dd, -1, DATEDIFF(dd, 0, GETDATE())) WHERE idLista = @lista";
                db.Database.ExecuteSqlCommand(sql,
                    new SqlParameter("lista", @lista));
            }
        }
    }
}