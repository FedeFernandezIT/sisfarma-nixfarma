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
                    return 1;

                var count = 0;
                while (reader.Read())
                {
                    count++;
                }

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