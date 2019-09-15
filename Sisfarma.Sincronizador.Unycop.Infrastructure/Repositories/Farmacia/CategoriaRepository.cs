using System;
using System.Data.OleDb;
using System.Linq;
using Oracle.DataAccess.Client;
using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public interface ICategoriaRepository
    {
        Categoria GetOneOrDefaultById(string id);
    }

    public class CategoriaRepository : FarmaciaRepository, ICategoriaRepository
    {
        public CategoriaRepository(LocalConfig config) : base(config)
        { }

        public CategoriaRepository()
        { }

        public Categoria GetOneOrDefaultById(string id)
        {
            string connectionString = @"User Id=""CONSU""; Password=""consu"";" +
                @"Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=IPC)(KEY=DP9))" +
                    "(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.0.30)(PORT=1521)))(CONNECT_DATA=(INSTANCE_NAME=DP9)(SERVICE_NAME=ORACLE9)))";

            var conn = new OracleConnection(connectionString);

            try
            {
                conn.Open();
                var sql =
                $@"SELECT c.DESCRIPCION FROM appul.ab_categorias c
                    INNER JOIN appul.ab_fichas f ON f.cte_codigo = c.codigo
                    WHERE f.art_codigo = '{id}'";
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                var nombre = string.Empty;
                if (reader.Read())
                    nombre = Convert.ToString(reader["DESCRIPCION"]) ?? string.Empty;

                return new Categoria { Nombre = nombre };
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
    }
}