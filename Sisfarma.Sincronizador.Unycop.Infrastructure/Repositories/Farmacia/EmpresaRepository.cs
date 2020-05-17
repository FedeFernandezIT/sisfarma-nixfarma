using System;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public interface IEmpresaRepository
    {
        int Count();
    }

    public class EmpresaRepository : IEmpresaRepository
    {
        public int Count()
        {
            var conn = FarmaciaContext.GetConnection();
            try
            {
                var sql = $@"SELECT distinct emp_codigo FROM appul.ab_articulos";
                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                if (!reader.HasRows)
                {
                    reader.Close();
                    reader.Dispose();
                    return 1;
                }
                    

                var count = 0;
                while (reader.Read())
                {
                    count++;
                }

                reader.Close();
                reader.Dispose();
                return count;
            }
            catch (Exception)
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