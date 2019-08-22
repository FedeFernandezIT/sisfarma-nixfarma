using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Data;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Linq;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia
{
    public class ListaRepository : FarmaciaRepository , IListaRepository
    {        
        public ListaRepository(LocalConfig config) : base(config)
        { }

        public ListaRepository() { }

        public IEnumerable<Lista> GetAllByIdGreaterThan(int id)
        {            
            using (var db = FarmaciaContext.Proveedores())
            {
                var sql = @"SELECT ID_Bolsa as Id, Descripcion FROM DescrBolsas WHERE id_bolsa > @id";
                var rs = db.Database.SqlQuery<Lista>(sql,
                    new OleDbParameter("id", id))
                    //.Take(1000)
                    .ToList();

                foreach (var item in rs)
                {
                    var sqlFarmacos = @"SELECT id_bolsa as ListaId, id_farmaco as FarmacoId FROM Bolsas WHERE id_bolsa = @id GROUP BY id_bolsa, id_farmaco";
                    var farmacos = db.Database.SqlQuery<ListaDetalle>(sqlFarmacos,
                        new OleDbParameter("id", item.Id))
                            .ToList();

                    item.Farmacos = farmacos;
                }

                return rs;
            }
        }        
    }
}