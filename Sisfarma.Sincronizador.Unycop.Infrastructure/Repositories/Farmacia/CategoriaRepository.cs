using System;
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
            var conn = FarmaciaContext.GetConnection();

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
                conn.Dispose();
            }
        }
    }
}