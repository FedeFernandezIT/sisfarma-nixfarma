using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Fisiotes.Models;
using System;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Fisiotes.Repositories
{
    public class EncargosRepository : FisiotesRepository
    {
        public EncargosRepository(FisiotesContext ctx) : base(ctx)
        {
        }

        public EncargosRepository(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        {
        }

        public Encargo LastOrDefault()
        {
            try
            {
                return _restClient
                .Resource(_config.Encargos.Ultimo)
                .SendGet<Encargo>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public bool Exists(int encargo)
        {
            return GetByEncargoOrDefault(encargo) != null;
        }

        public Encargo GetByEncargoOrDefault(int encargo)
        {
            try
            {
                return _restClient
                    .Resource(_config.Encargos.GetByEncargo.Replace("{encargo}", $"{encargo}"))
                    .SendGet<Encargo>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public void UpdateFechaDeRecepcion(DateTime fechaRecepcion, long idEncargo)
        {
            var encargo = new
            {
                idEncargo = idEncargo,
                fechaRecepcion = fechaRecepcion.ToIsoString()                
            };

            _restClient
                .Resource(_config.Encargos.Update)
                .SendPost(new
                {
                    bulk = new[] { encargo }
                });
        }

        public void UpdateFechaDeEntrega(DateTime fechaEntrega, long idEncargo)
        {
            var encargo = new
            {
                idEncargo = idEncargo,
                fechaEntrega = fechaEntrega.ToIsoString()
            };

            _restClient
                .Resource(_config.Encargos.Update)
                .SendPost(new
                {
                    bulk = new[] { encargo }
                });
        }
                
                
        public void Insert(Encargo ee)
        {
            var encargo =  new
            {
                idEncargo = ee.idEncargo,
                cod_nacional = ee.cod_nacional,
                nombre = ee.nombre,
                familia = ee.familia,
                superFamilia = ee.superFamilia,
                cod_laboratorio = ee.cod_laboratorio,
                laboratorio = ee.laboratorio,
                proveedor = ee.proveedor,
                pvp = ee.pvp,
                puc = ee.puc,
                dni = ee.dni,
                fecha = ee.fecha.ToIsoString(),
                trabajador = ee.trabajador,
                unidades = ee.unidades,
                fechaEntrega = ee.fechaEntrega.ToIsoString(),
                observaciones = ee.observaciones
            };

            _restClient
                .Resource(_config.Encargos.Insert)
                .SendPost(new
                {
                    bulk = new[] { encargo }
                });
        }

        #region SQL Methods
        public void CheckAndCreateProveedorField()
        {
            const string table = @"SELECT * from encargos LIMIT 0,1;";
            var fields = new[] { "proveedor" };
            var alters = new[]
            {
                @"ALTER TABLE encargos ADD proveedor VARCHAR(255) DEFAULT NULL AFTER laboratorio;"
            };
            CheckAndCreateFieldsTemplate(table, fields, alters);
        }

        public Encargo LastSql()
        {
            var sql = @"select * from encargos order by idEncargo Desc Limit 0,1";
            return _ctx.Database.SqlQuery<Encargo>(sql)
                .FirstOrDefault();
        }

        public Encargo GetSql(int encargo)
        {
            var sql = @"select * from encargos where IdEncargo = @encargo";
            return _ctx.Database.SqlQuery<Encargo>(sql,
                new SqlParameter("encargo", encargo))
                .FirstOrDefault();
        }

        public void InsertSql(long? encargo, string codNacional, string nombre, string superFamilia, string familia, string codLaboratorio,
            string laboratorio, string proveedor, float? pvp, float? puc, string dni, DateTime? fecha, string trabajador, int? unidades, DateTime? fechaEntrega,
            string observaciones)
        {
            var sql = @"INSERT IGNORE INTO encargos (idEncargo, cod_nacional, nombre, superFamilia, familia, cod_laboratorio, laboratorio, proveedor, pvp, puc, dni, " +
                        @"fecha, trabajador, unidades, fechaEntrega, observaciones) VALUES(" +
                        @"@encargo, @codNacional, @nombre, @superFamilia, @familia, @codLaboratorio, @laboratorio, @proveedor, @pvp, @puc, @dni, " +
                        @"@fecha, @trabajador, @unidades, @fechaEntrega, @observaciones)";
            _ctx.Database.ExecuteSqlCommand(sql,
                new SqlParameter("encargo", encargo),
                new SqlParameter("codNacional", codNacional),
                new SqlParameter("nombre", nombre),
                new SqlParameter("superFamilia", superFamilia),
                new SqlParameter("familia", familia),
                new SqlParameter("codLaboratorio", codLaboratorio),
                new SqlParameter("laboratorio", laboratorio),
                new SqlParameter("proveedor", proveedor),
                new SqlParameter("pvp", pvp),
                new SqlParameter("puc", puc),
                new SqlParameter("dni", dni),
                new SqlParameter("fecha", fecha),
                new SqlParameter("trabajador", trabajador),
                new SqlParameter("unidades", unidades),
                new SqlParameter("fechaEntrega", fechaEntrega),
                new SqlParameter("observaciones", observaciones));
        }

        #endregion
    }
}