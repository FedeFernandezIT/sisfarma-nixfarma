using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Fisiotes.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Fisiotes.Repositories
{
    public class FaltasRepository : FisiotesRepository
    {
        public FaltasRepository(FisiotesContext ctx) : base(ctx)
        {
        }

        public FaltasRepository(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        {
        }

        public Falta LastOrDefault()
        {
            try
            {
                return _restClient
                .Resource(_config.Faltas.Ultima)
                .SendGet<Falta>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public Falta GetByLineaDePedido(int pedido, int linea)
        {
            try
            {
                return _restClient
                .Resource(_config.Faltas.GetByLineaDePedido
                    .Replace("{pedido}", $"{pedido}")
                    .Replace("{linea}", $"{linea}"))
                .SendGet<Falta>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public bool ExistsLineaDePedido(int idPedido, int idLinea)
        {
            return GetByLineaDePedido(idPedido, idLinea) != null;
        }

        public void Insert(Falta ff)
        {
            var falta = new
            {
                idPedido = ff.idPedido,
                idLinea = ff.idLinea,
                cod_nacional = ff.cod_nacional,
                descripcion = ff.descripcion,
                familia = ff.familia,
                superFamilia = ff.superFamilia,
                cantidadPedida = ff.cantidadPedida,
                fechaFalta = ff.fechaFalta.ToIsoString(),
                cod_laboratorio = ff.cod_laboratorio,
                laboratorio = ff.laboratorio,
                proveedor = ff.proveedor,
                fechaPedido = ff.fechaPedido.ToIsoString(),
                pvp = ff.pvp,
                puc = ff.puc                
            };

            _restClient
                .Resource(_config.Faltas.InsertLineaDePedido)
                .SendPost(new
                {
                    bulk = new[] { falta }
                });
        }


        public void CheckAndCreateProveedorField()
        {
            const string table = @"SELECT * from faltas LIMIT 0,1;";
            var fields = new[] { "proveedor" };
            var alters = new[]
            {
                "ALTER TABLE faltas ADD proveedor VARCHAR(255) DEFAULT NULL AFTER laboratorio;"
            };
            CheckAndCreateFieldsTemplate(table, fields, alters);
        }
        
        
        #region SQL Methods

        public Falta LastSql()
        {
            var sql = "select * from faltas order by idPedido Desc Limit 0,1";
            return _ctx.Database.SqlQuery<Falta>(sql)
                .FirstOrDefault();
        }

        public Falta GetByLineaPedidoSql(int pedido, int linea)
        {
            var sql = @"select * from faltas where idPedido = @pedido AND idLinea= @linea";
            return _ctx.Database.SqlQuery<Falta>(sql,
                new SqlParameter("pedido", pedido),
                new SqlParameter("linea", linea))
                .FirstOrDefault();
        }

        public void InsertSql(int pedido, int linea, string codNacional, string descripcion, string familia, string superFamilia, int cantidad,
            DateTime fechaFalta, string codLaboratorio, string nombreLaboratorio, string proveedor, DateTime? fechaPedido, float pvp, float puc)
        {
            var sql = @"INSERT IGNORE INTO faltas (idPedido, idLinea, cod_nacional, descripcion, familia, superFamilia, cantidadPedida, fechaFalta, " +
                        "cod_laboratorio, laboratorio, proveedor, fechaPedido, pvp, puc) VALUES(" +
                        "@pedido, @linea, @codNacional, @descripcion, @familia, @superFamilia, @cantidad, @fechaFalta, @codLaboratorio, " +
                        "@nombreLaboratorio, @proveedor, @fechaPedido, @pvp, @puc)";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("pedido", pedido),
                new SqlParameter("linea", linea),
                new SqlParameter("codNacional", codNacional),
                new SqlParameter("descripcion", descripcion),
                new SqlParameter("familia", familia),
                new SqlParameter("superFamilia", superFamilia),
                new SqlParameter("cantidad", cantidad),
                new SqlParameter("fechaFalta", fechaFalta),
                new SqlParameter("codLaboratorio", codLaboratorio),
                new SqlParameter("nombreLaboratorio", nombreLaboratorio),
                new SqlParameter("proveedor", proveedor),
                new SqlParameter("fechaPedido", fechaPedido),
                new SqlParameter("pvp", pvp),
                new SqlParameter("puc", puc));
        }

        #endregion
    }
}