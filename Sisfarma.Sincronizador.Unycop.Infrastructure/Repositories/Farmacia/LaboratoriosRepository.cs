using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public class LaboratorioRepository : FarmaciaRepository, ILaboratorioRepository
    {
        protected const string LABORATORIO_DEFAULT = "<Sin Laboratorio>";

        public LaboratorioRepository(LocalConfig config) : base(config)
        { }

        public LaboratorioRepository()
        {
        }

        public Laboratorio GetOneOrDefaultByCodigo(long codigo, string clase, string claseBot)
        {
            var conn = FarmaciaContext.GetConnection();

            try
            {
                conn.Open();
                var sql = $@"SELECT CODIGO, NOMBRE FROM appul.ab_laboratorios WHERE codigo = {codigo} AND clase = '{clase}' AND clase_bot = '{claseBot}'";
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                var nombre = string.Empty;
                var numeroLaboratorio = 0L;
                if (reader.Read())
                {
                    numeroLaboratorio = Convert.ToInt64(reader["CODIGO"]);
                    nombre = Convert.ToString(reader["NOMBRE"]) ?? string.Empty;

                    var letraLaboratorio = clase != "1" ? "P"
                        : claseBot == "V" ? "V"
                        : "E";

                    reader.Close();
                    reader.Dispose();

                    return new Laboratorio
                    {
                        Codigo = letraLaboratorio + $"{numeroLaboratorio}".PadLeft(4, '0'),
                        Nombre = nombre
                    };
                }

                reader.Close();
                reader.Dispose();

                return new Laboratorio { Codigo = string.Empty, Nombre = LABORATORIO_DEFAULT };
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