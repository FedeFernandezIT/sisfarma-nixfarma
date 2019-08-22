using Sisfarma.RestClient;
using System;
using System.Data;
using System.Linq;

namespace Sisfarma.Sincronizador.Fisiotes.Repositories
{
    public abstract class FisiotesRepository
    {
        protected FisiotesContext _ctx;
        protected IRestClient _restClient;
        protected FisiotesConfig _config;

        public FisiotesRepository(FisiotesContext ctx)
        {
            _ctx = ctx;
        }

        public FisiotesRepository(IRestClient restClient, FisiotesConfig config)
        {
            _restClient = restClient ?? throw new ArgumentNullException(nameof(restClient));
            _config = config ?? throw new ArgumentNullException(nameof(config));

            _restClient.BaseAddress(_config.BaseAddress)
                .UseAuthenticationBasic(_config.Credentials.Token);
        }

        protected void CheckAndCreateFieldsTemplate(string sqlTable, string[] fields, string[] sqlAlter)
        {
            // Por defecto todos false
            var existsFields = new bool[fields.Length];

            using (var ctx = new FisiotesContext())
            {
                // Chekeamos si existen los campos
                var connection = ctx.Database.Connection;
                var sql = sqlTable;
                var command = connection.CreateCommand();
                command.CommandText = sql;
                connection.Open();
                var reader = command.ExecuteReader();
                var schemaTable = reader.GetSchemaTable();

                foreach (DataRow row in schemaTable.Rows)
                {
                    // Verifcamos los campos en el schema
                    for (var i = 0; i < fields.Length; i++)
                    {
                        if (row[schemaTable.Columns["ColumnName"]].ToString().Equals(fields[i]))
                            existsFields[i] = true;
                    }
                    if (existsFields.All(x => x))
                        break;
                }
                connection.Close();

                for (var i = 0; i < existsFields.Length; i++)
                {
                    if (!existsFields[i])
                        ctx.Database.ExecuteSqlCommand(sqlAlter[i]);
                }
            }
        }
    }
}