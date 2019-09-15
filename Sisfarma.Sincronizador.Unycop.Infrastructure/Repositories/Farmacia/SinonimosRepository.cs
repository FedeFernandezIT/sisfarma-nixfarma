using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public class SinonimosRepository : FarmaciaRepository, ISinonimosRepository
    {
        public SinonimosRepository(LocalConfig config) : base(config)
        { }

        public SinonimosRepository()
        {
        }

        public IEnumerable<Sinonimo> GetAll()
        {
            var conn = FarmaciaContext.GetConnection();
            var sinonimos = new List<Sinonimo>();
            try
            {
                conn.Open();
                var sql = $@"SELECT * FROM appul.ad_crelativos";
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var rCodRelativo = Convert.ToString(reader["COD_RELATIVO"]);
                    var rArtCodigo = Convert.ToString(reader["ART_CODIGO"]);
                    sinonimos.Add(new Sinonimo { CodigoBarra = rCodRelativo, CodigoNacional = rArtCodigo });
                }

                sql = $@"SELECT codigo, ean_13 FROM appul.ab_articulos where not ean_13 is null group by codigo, ean_13";
                cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var rCodigo = Convert.ToString(reader["codigo"]);
                    var rEan13 = Convert.ToString(reader["ean_13"]);
                    sinonimos.Add(new Sinonimo { CodigoBarra = rEan13, CodigoNacional = rCodigo });
                }

                return sinonimos;
            }
            catch (Exception ex)
            {
                return new List<Sinonimo>();
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }
    }
}