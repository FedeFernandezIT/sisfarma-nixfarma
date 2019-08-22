using Sisfarma.Sincronizador.Config;
using Sisfarma.Sincronizador.Farmatic.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Farmatic.Repositories
{
    public class ClientesRepository : FarmaticRepository
    {
        public ClientesRepository(FarmaticContext ctx) : base(ctx)
        { }

        public ClientesRepository(LocalConfig config) : base(config)
        { }

        public List<Cliente> GetGreatThanId(int id)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql =
                @"SELECT TOP 1000 * FROM cliente WHERE Idcliente > @ultimoCliente ORDER BY CAST(Idcliente AS DECIMAL(20)) ASC";
                return db.Database.SqlQuery<Cliente>(sql,
                    new SqlParameter("ultimoCliente", id))
                    .ToList();
            }
        }

        public T GetAuxiliarById<T>(string cliente) where T : ClienteAux
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM ClienteAux WHERE idCliente = @idCliente";
                return db.Database.SqlQuery<T>(sql,
                    new SqlParameter("idCliente", cliente))
                    .FirstOrDefault();
            }
        }

        public decimal GetTotalPuntosById(string idCliente)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT ISNULL(SUM(cantidad), 0) AS puntos FROM HistoOferta WHERE IdCliente = @idCliente AND TipoAcumulacion = 'P'";
                return db.Database.SqlQuery<decimal>(sql,
                    new SqlParameter("idCliente", idCliente))
                    .FirstOrDefault();
            }
        }

        public bool HasSexoField()
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var existFieldSexo = false;

                // Chekeamos si existen los campos
                var connection = db.Database.Connection;

                var sql = "SELECT TOP 1 * FROM ClienteAux";
                var command = connection.CreateCommand();
                command.CommandText = sql;
                connection.Open();
                var reader = command.ExecuteReader();
                var schemaTable = reader.GetSchemaTable();

                foreach (DataRow row in schemaTable.Rows)
                {
                    if (row[schemaTable.Columns["ColumnName"]].ToString()
                        .Equals("sexo", StringComparison.CurrentCultureIgnoreCase))
                    {
                        existFieldSexo = true;
                        break;
                    }
                }
                connection.Close();
                return existFieldSexo;
            }
        }

        public Cliente GetOneOrDefaulById(int dni)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM Cliente WHERE Idcliente = @dni";
                return db.Database.SqlQuery<Cliente>(sql,
                    new SqlParameter("dni", dni))
                    .FirstOrDefault();
            }
        }

        public bool Exists(int id)
            => GetOneOrDefaulById(id) != null;

        public bool EsBeBlue(string tipoCliente)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT descripcion FROM tipocliente WHERE idtipocliente = @tipoCliente";
                var tipo = db.Database.SqlQuery<TipoCliente>(sql,
                    new SqlParameter("tipoCliente", tipoCliente))
                    .FirstOrDefault();

                if (tipo == null)
                    return false;

                return tipo.Descripcion.Trim().ToLower() == "farmazul";
            }
        }
    }
}