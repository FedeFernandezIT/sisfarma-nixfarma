using Sisfarma.Sincronizador.Config;
using System;
using System.Data.Entity;
using System.Data.Entity.Core.EntityClient;
using System.Data.SqlClient;

namespace Sisfarma.Sincronizador.Farmatic
{
    public class FarmaticContext : DbContext
    {
        public FarmaticContext()
            : base("FarmaticContext")
        {
        }

        public FarmaticContext(string server, string database, string username, string password)
            //: base(ConnectToSqlServer(server, database, username, password))
            : base($@"data source={server}; initial catalog={database}; persist security info=True;user id={username}; password={password};MultipleActiveResultSets=True;App=EntityFramework")
        {
        }

        public static FarmaticContext Create(LocalConfig config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            return new FarmaticContext(config.Server, config.Database, config.Username, config.Password);
        }

        public static string ConnectToSqlServer(string hostServer, string catalogDbName, string user, string pass)
        {
            SqlConnectionStringBuilder sqlBuilder = new SqlConnectionStringBuilder
            {
                DataSource = hostServer,
                InitialCatalog = catalogDbName,
                PersistSecurityInfo = true,
                IntegratedSecurity = false,
                MultipleActiveResultSets = true,

                UserID = user,
                Password = pass,
            };

            // assumes a connectionString name in .config of MyDbEntities
            var entityConnectionStringBuilder = new EntityConnectionStringBuilder
            {
                Provider = "System.Data.SqlClient",
                ProviderConnectionString = sqlBuilder.ConnectionString,
                //Metadata = "res://*/",
                // or
                //Metadata = "res://*/DbModel.csdl|res://*/DbModel.ssdl|res://*/DbModel.msl",
            };

            ////  //works though
            //using (EntityConnection conn =
            //        new EntityConnection(entityConnectionStringBuilder.ToString()))
            //{
            //    conn.Open();
            //    Console.WriteLine("Just testing the connection.");
            //    conn.Close();
            //}

            // return entityConnectionStringBuilder.ConnectionString;
            var connString = entityConnectionStringBuilder.ToString();
            //return connString;
            return sqlBuilder.ConnectionString;
        }
    }
}