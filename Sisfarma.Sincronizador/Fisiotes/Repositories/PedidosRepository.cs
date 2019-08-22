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
    public class PedidosRepository : FisiotesRepository
    {
        public PedidosRepository(FisiotesContext ctx) : base(ctx)
        {
        }

        public PedidosRepository(IRestClient restClient, FisiotesConfig config) 
            : base(restClient, config)
        {
        }

        public Pedido LastOrDefault()
        {
            try
            {
                return _restClient
                .Resource(_config.Pedidos.Ultimo)
                .SendGet<Pedido>();                    
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }            
        }


        public bool Exists(int pedido)
        {
            return Get(pedido) != null;
        }

        public Pedido Get(int pedido)
        {
            try
            {
                return _restClient
                .Resource(_config.Pedidos.GetByPedido.Replace("{pedido}", $"{pedido}"))
                .SendGet<Pedido>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public void Insert(Pedido pp)
        {
            var pedido = new
            {
                idPedido = pp.idPedido,
                fechaPedido = pp.fechaPedido.ToIsoString(),
                hora = pp.hora.ToIsoString(),
                numLineas = pp.numLineas,
                importePvp = pp.importePvp,
                importePuc = pp.importePuc,
                idProveedor = pp.idProveedor,
                proveedor = pp.proveedor,
                trabajador = pp.trabajador
            };

            _restClient
                .Resource(_config.Pedidos.Insert)
                .SendPost(new
                {
                    bulk = new[] { pedido }
                });            
        }


        public bool ExistsLinea(int pedido, int linea)
        {
            return GetLineaByKey(pedido, linea) != null;
        }

        public LineaPedido GetLineaByKey(int pedido, int linea)
        {
            try
            {
                return _restClient
                .Resource(_config.Pedidos.GetByLineaDePedido
                    .Replace("{pedido}", $"{pedido}")
                    .Replace("{linea}", $"{linea}"))
                .SendGet<LineaPedido>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }            
        }


        public void InsertLinea(LineaPedido ll)
        {
            var linea = new
            {
                fechaPedido = ll.fechaPedido.ToIsoString(),
                idPedido = ll.idPedido,
                idLinea = ll.idLinea,
                cod_nacional = ll.cod_nacional,
                descripcion = ll.descripcion,
                familia = ll.familia,
                superFamilia = ll.superFamilia,
                cantidad = ll.cantidad,
                pvp = ll.pvp,
                puc = ll.puc,
                cod_laboratorio = ll.cod_laboratorio,
                laboratorio = ll.laboratorio
            };

            _restClient
                .Resource(_config.Pedidos.InsertLineaDePedido)
                .SendPost(new
                {
                    bulk = new[] { linea }
                });
        }

        

        public void CreateTable(string remote)
        {
            var sql =
                @"SELECT TABLE_NAME AS tipo From information_schema.TABLES WHERE TABLE_SCHEMA = @baseRemoto AND TABLE_NAME = 'pedidos'";
            var result = _ctx.Database.SqlQuery<string>(sql,
                new SqlParameter("baseRemoto", remote))
                .ToList();
            if (result.Count == 0)
            {
                sql = "CREATE TABLE IF NOT EXISTS `lineas_pedidos` (" +
                        "`id` bigint(255) unsigned NOT NULL AUTO_INCREMENT," +
                        "`fechaPedido` datetime DEFAULT NULL," +
                        "`idPedido` bigint(255) DEFAULT NULL," +
                        "`idLinea` bigint(255) DEFAULT NULL," +
                        "`cod_nacional` bigint(255) DEFAULT NULL," +
                        "`descripcion` varchar(255) DEFAULT NULL," +
                        "`familia` varchar(255) DEFAULT NULL," +
                        "`superFamilia` varchar(255) DEFAULT NULL," +
                        "`cantidad` int(11) DEFAULT NULL," +
                        "`pvp` float DEFAULT NULL," +
                        "`puc` float DEFAULT NULL," +
                        "`cod_laboratorio` varchar(50) DEFAULT NULL," +
                        "`laboratorio` varchar(255) DEFAULT NULL," +
                        "PRIMARY KEY (`id`)" +
                        ") ENGINE=MyISAM DEFAULT CHARSET=latin1;";
                _ctx.Database.ExecuteSqlCommand(sql);

                sql = "CREATE TABLE IF NOT EXISTS `pedidos` (" +
                        "`id` bigint(255) unsigned NOT NULL AUTO_INCREMENT," +
                        "`idPedido` bigint(255) DEFAULT NULL," +
                        "`fechaPedido` datetime DEFAULT NULL," +
                        "`hora` datetime DEFAULT NULL," +
                        "`numLineas` int(11) DEFAULT NULL," +
                        "`importePvp` float DEFAULT NULL," +
                        "`importePuc` float DEFAULT NULL," +
                        "`idProveedor` varchar(50) DEFAULT NULL," +
                        "`proveedor` varchar(255) DEFAULT NULL," +
                        "`trabajador` varchar(255) DEFAULT NULL," +
                        "`sistema` varchar(50) DEFAULT NULL," +
                        "PRIMARY KEY (`id`)" +
                        ") ENGINE=MyISAM DEFAULT CHARSET=latin1;";
                _ctx.Database.ExecuteSqlCommand(sql);
            }
        }

        public void CheckAndCreateFechaPedidoField()
        {
            const string table = @"SELECT * from lineas_pedidos LIMIT 0,1;";
            var fields = new[] { "fechaPedido" };
            var alters = new[]
            {
                @"ALTER TABLE lineas_pedidos ADD fechaPedido DATETIME AFTER id;"
            };
            CheckAndCreateFieldsTemplate(table, fields, alters);
        }
        
        
        #region SQL Methods
        
        public Pedido LastOrDefaultSql()
        {
            var sql = @"select * from pedidos order by idPedido Desc Limit 0,1";
            return _ctx.Database.SqlQuery<Pedido>(sql)
                .FirstOrDefault();
        }

        public Pedido GetSql(int pedido)
        {
            var sql = @"select * from pedidos where idPedido = @pedido";
            return _ctx.Database.SqlQuery<Pedido>(sql,
                new SqlParameter("pedido", pedido))
                .FirstOrDefault();
        }

        public void InsertSql(int idPedido, DateTime fechaPedido, DateTime hora, int numLineas, float importePvp, float importePuc, string idProveedor, string proveedor, string trabajador)
        {
            var sql = @"INSERT IGNORE INTO pedidos (idPedido,fechaPedido,hora,numLineas,importePvp,importePuc,idProveedor,proveedor,trabajador) VALUES(" +
                            @"@idPedido, @fechaPedido, @hora, @numLineas, @importePvp, @importePuc, @idProveedor, @proveedor, @trabajador)";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("idPedido", idPedido),
                new SqlParameter("fechaPedido", fechaPedido),
                new SqlParameter("hora", hora),
                new SqlParameter("numLineas", numLineas),
                new SqlParameter("importePvp", importePvp),
                new SqlParameter("importePuc", importePuc),
                new SqlParameter("idProveedor", idProveedor),
                new SqlParameter("proveedor", proveedor),
                new SqlParameter("trabajador", trabajador));
        }

        public LineaPedido GetLineaByKeySql(int pedido, int linea)
        {
            var sql = @"select * from lineas_pedidos where idPedido = @pedido AND idLinea= @linea";
            return _ctx.Database.SqlQuery<LineaPedido>(sql,
                new SqlParameter("pedido", pedido),
                new SqlParameter("linea", linea))
                .FirstOrDefault();
        }

        public void InsertLineaSql(DateTime fechaPedido, int idPedido, int idLinea, string codNacional, string descripcion, string familia,
            string superFamilia, int cantidad, float pvp, float puc, string codLaboratorio, string laboratorio)
        {
            var sql = @"INSERT IGNORE INTO lineas_pedidos (fechaPedido, idPedido, idLinea, cod_nacional, descripcion, familia, superFamilia, cantidad, pvp, puc, cod_laboratorio, laboratorio) VALUES(" +
                    "@fechaPedido, @idPedido, @idLinea, @codNacional, @descripcion, @familia, @superFamilia, @cantidad, @pvp, @puc, @codLaboratorio, @laboratorio)";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("fechaPedido", fechaPedido),
                new SqlParameter("idPedido", idPedido),
                new SqlParameter("idLinea", idLinea),
                new SqlParameter("codNacional", codNacional),
                new SqlParameter("descripcion", descripcion),
                new SqlParameter("familia", familia),
                new SqlParameter("superFamilia", superFamilia),
                new SqlParameter("cantidad", cantidad),
                new SqlParameter("pvp", pvp),
                new SqlParameter("puc", puc),
                new SqlParameter("codLaboratorio", codLaboratorio),
                new SqlParameter("laboratorio", laboratorio));
        }
        #endregion
    }
}