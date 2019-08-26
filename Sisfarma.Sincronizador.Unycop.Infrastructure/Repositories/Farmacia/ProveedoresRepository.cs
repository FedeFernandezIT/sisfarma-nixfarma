using Oracle.DataAccess.Client;
using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public class ProveedoresRepository : FarmaciaRepository, IProveedorRepository
    {
        private readonly IRecepcionRespository _recepcionRespository;

        public ProveedoresRepository(LocalConfig config,
            IRecepcionRespository recepcionRespository) : base(config)
        {
            _recepcionRespository = recepcionRespository ?? throw new System.ArgumentNullException(nameof(recepcionRespository));
        }

        public ProveedoresRepository(IRecepcionRespository recepcionRespository)
        {
            _recepcionRespository = recepcionRespository ?? throw new System.ArgumentNullException(nameof(recepcionRespository));
        }

        public Proveedor GetOneOrDefaultById(long id)
        {
            var idInteger = (int)id;
            using (var db = FarmaciaContext.Proveedores())
            {
                var sql = "SELECT ID_Proveedor as Id, Nombre FROM Proveedores WHERE ID_Proveedor = @id";
                return db.Database.SqlQuery<Proveedor>(sql,
                    new OleDbParameter("id", idInteger))
                    .FirstOrDefault();
            }
        }

        public Proveedor GetOneOrDefaultByCodigoNacional(string codigoNacional)
        {
            string connectionString = @"User Id=""CONSU""; Password=""consu"";" +
                @"Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=IPC)(KEY=DP9))" +
                    "(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.0.30)(PORT=1521)))(CONNECT_DATA=(INSTANCE_NAME=DP9)(SERVICE_NAME=ORACLE9)))";

            var conn = new OracleConnection(connectionString);

            try
            {
                conn.Open();
                var sql = $@"SELECT PROVEEDOR
                    FROM (
                        SELECT PROVEEDOR FROM appul.ad_rec_linped
                            WHERE cant_servida <> 0 AND art_codigo = '{codigoNacional}'
                        ORDER BY fecha_recepcion DESC)
                    WHERE ROWNUM <= 1";

                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                if (reader.Read() && !Convert.IsDBNull(reader["PROVEEDOR"]))
                {
                    var id = Convert.ToInt64(reader["PROVEEDOR"]);
                    sql = $@"SELECT NOMBRE_AB from appul.ad_proveedores where codigo = {id}";
                    cmd.CommandText = sql;
                    reader = cmd.ExecuteReader();
                    if (reader.Read())
                    {
                        var nombre = Convert.ToString(reader["NOMBRE_AB"]);
                        return new Proveedor
                        {
                            Id = id,
                            Nombre = nombre
                        };
                    }
                }

                return new Proveedor { Nombre = string.Empty };
            }
            catch (Exception ex)
            {
                return null;
            }
            finally
            {
                conn.Close();
            }
        }

        public IEnumerable<Proveedor> GetAll()
        {
            using (var db = FarmaciaContext.Proveedores())
            {
                var sql = "SELECT ID_Proveedor as Id, Nombre FROM proveedores";
                return db.Database.SqlQuery<Proveedor>(sql)
                    .ToList();
            }
        }
    }
}