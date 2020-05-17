using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public class CategoriasRepository : FarmaciaRepository, ICategoriasRepository
    {
        public CategoriasRepository(LocalConfig config) : base(config)
        { }

        public CategoriasRepository()
        {
        }

        public IEnumerable<Categoria> GetAll()
        {
            var conn = FarmaciaContext.GetConnection();
            var categorias = new List<Categoria>();
            try
            {
                conn.Open();
                var sql = $@"select * from appul.ab_categorias";
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var descripcion = Convert.ToString(reader["DESCRIPCION"]) ?? string.Empty;
                    categorias.Add(new Categoria { Nombre = descripcion });
                }

                return categorias;
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }
    }
}