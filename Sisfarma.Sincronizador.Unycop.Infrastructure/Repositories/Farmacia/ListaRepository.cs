using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Linq;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public class ListaRepository : FarmaciaRepository, IListaRepository
    {
        public ListaRepository(LocalConfig config) : base(config)
        { }

        public ListaRepository()
        {
        }

        public IEnumerable<Lista> GetAllByIdGreaterThan(long id)
        {
            var conn = FarmaciaContext.GetConnection();
            var listas = new List<Lista>();
            try
            {
                conn.Open();
                var sql = $@"SELECT * FROM appul.aa_filtros WHERE codigo > {id}";
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var rCodigo = Convert.ToInt64(reader["CODIGO"]);
                    var rTituloCliente = Convert.ToString(reader["TITULO_CLIENTE"]);
                    var rNumRegistro = !Convert.IsDBNull(reader["NUM_REGISTRO"]) ? Convert.ToInt64(reader["NUM_REGISTRO"]) : 0L;

                    var lista = new Lista
                    {
                        Id = rCodigo,
                        NumElem = rNumRegistro,
                        Descripcion = rTituloCliente
                    };

                    listas.Add(lista);

                    sql = $@"SELECT valor_char FROM appul.aa_filtros_det WHERE flt_codigo = {rCodigo} GROUP BY ROLLUP(valor_char)";
                    cmd = conn.CreateCommand();
                    cmd.CommandText = sql;
                    var readerDetalle = cmd.ExecuteReader();
                    var numeroRegistro = 0;
                    while (readerDetalle.Read())
                    {
                        var rValorChar = Convert.ToString(reader["valor_char"]);

                        var item = new ListaDetalle
                        {
                            Id = ++numeroRegistro,
                            FarmacoId = rValorChar,
                            ListaId = rCodigo
                        };
                        lista.Farmacos.Add(item);
                    }

                    listas.Add(lista);
                }

                return listas;
            }
            catch (Exception ex)
            {
                return new List<Lista>();
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }
    }
}