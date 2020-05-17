using System;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public interface ITarifaRepository
    {
        decimal? GetTarifaOrDefaultByCodigoImpuesto(string impuesto);
    }

    public class TarifaRepository : ITarifaRepository
    {
        public decimal? GetTarifaOrDefaultByCodigoImpuesto(string impuesto)
        {
            var conn = FarmaciaContext.GetConnection();
            try
            {
                var sql = $@"SELECT valor_imp FROM appul.gn_tarifas_imp WHERE imp_codigo = '{impuesto}' ORDER BY fecha_inicio DESC";

                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                if (reader.Read())
                {
                    var rValorImp = !Convert.IsDBNull(reader["valor_imp"]) ? (decimal?)Convert.ToDecimal(reader["valor_imp"]) : null;
                    
                    reader.Close();
                    reader.Dispose();
                    
                    return rValorImp;
                }

                reader.Close();
                reader.Dispose();

                return null;
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