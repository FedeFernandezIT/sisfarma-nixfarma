using Oracle.DataAccess.Client;
using Sisfarma.Sincronizador.Core.Config;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.IO;
using System.Linq;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data
{
    public class FarmaciaContext : DbContext
    {
        private OracleConnection OracleConnection { get; set; }

        public FarmaciaContext(string server, string database, string username, string password)
            : base("OracleDbContext")
        //: base($@"providerName=""Oracle.ManagedDataAccess.Client"" connectionString=""User Id ={username}; Password={password};Data Source ={server}/{database}""")
        { }

        public static FarmaciaContext Create(LocalConfig config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            return new FarmaciaContext(config.Server, config.Database, config.Username, config.Password);
        }

        private static readonly string _pattern = @"Hst????.accdb";
        private static readonly string _server = "";
        private static readonly string _username = "";

        private static int _anioActual = 0;
        private static ICollection<int> _historicos;
        private static string _localServer = "";
        private static string _user = "";
        private static string _password = "";

        public FarmaciaContext()
        {
        }

        public static int ListaDeArticulo { get; set; }

        public static void Setup(string localServer, string user, string password, int listaDeArticulo)
        {
            if (string.IsNullOrWhiteSpace(localServer))
                throw new System.ArgumentException("message", nameof(localServer));

            _localServer = localServer;
            _user = user ?? throw new System.ArgumentNullException(nameof(user));
            _password = password ?? throw new System.ArgumentNullException(nameof(password));

            ListaDeArticulo = listaDeArticulo;
        }

        public static FarmaciaContext Default()
            => new FarmaciaContext(
                server: _localServer,
                database: "nixfarma",
                username: _user,
                password: _password);

        public static FarmaciaContext VentasByYear(int year)
        {
            var historicos = GetHistoricos();

            if (historicos.All(x => x > year))
                throw new FarmaciaContextException();

            if (_historicos.Contains(year))
            {
                return new FarmaciaContext(
                    server: _server,
                    database: $@"{_localServer}\Hst{year}.accdb",
                    username: _username,
                    password: _password);
            }

            return new FarmaciaContext(
                server: _server,
                database: $@"{_localServer}\Ventas.accdb",
                username: _username,
                password: _password);
        }

        private static ICollection<int> GetHistoricos()
        {
            if (_historicos == null)
            {
                var historicos = Directory.GetFiles(
                path: $@"{_localServer}",
                searchPattern: _pattern,
                searchOption: SearchOption.TopDirectoryOnly)
                    .Select(path => new string(path.Replace(".accdb", string.Empty).TakeLast(4).ToArray()))
                    .Where(yyyy => int.TryParse(yyyy, out var number))
                        .Select(anio => int.Parse(anio));

                _historicos = new HashSet<int>(historicos);
            }

            return _historicos;
        }

        public static FarmaciaContext Clientes()
        {
            return new FarmaciaContext(
                server: _server,
                database: $@"{_localServer}\Clientes.accdb",
                username: _username,
                password: _password);
        }

        public static FarmaciaContext Fidelizacion()
        {
            return new FarmaciaContext(
                server: _server,
                database: $@"{_localServer}\Fidelizacion.accdb",
                username: _username,
                password: _password);
        }

        public static FarmaciaContext Vendedor()
        {
            return new FarmaciaContext(
                server: _server,
                database: $@"{_localServer}\Vendedor.accdb",
                username: _username,
                password: _password);
        }

        public static FarmaciaContext Farmacos()
        {
            return new FarmaciaContext(
                server: _server,
                database: $@"{_localServer}\Farmacos.accdb",
                username: _username,
                password: _password);
        }

        public static FarmaciaContext Recepcion()
        {
            return new FarmaciaContext(
                server: _server,
                database: $@"{_localServer}\FarmaDen.accdb",
                username: _username,
                password: _password);
        }

        public static FarmaciaContext RecepcionByYear(int year)
        {
            var historicos = GetHistoricos();

            if (historicos.All(x => x > year))
                throw new FarmaciaContextException();

            if (_historicos.Contains(year))
            {
                return new FarmaciaContext(
                    server: _server,
                    database: $@"{_localServer}\Hst{year}.accdb",
                    username: _username,
                    password: _password);
            }

            return new FarmaciaContext(
                server: _server,
                database: $@"{_localServer}\FarmaDen.accdb",
                username: _username,
                password: _password);
        }

        public static FarmaciaContext Proveedores()
        {
            return new FarmaciaContext(
                server: _server,
                database: $@"{_localServer}\Proveedo.accdb",
                username: _username,
                password: _password);
        }

        public static OracleConnection GetConnection()
        {
            string connectionString = $@"User Id=""{_user.ToUpper()}""; Password=""{_password}"";" +
                @"Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=IPC)(KEY=DP9))" +
                    $@"(ADDRESS=(PROTOCOL=TCP)(HOST={_localServer})(PORT=1521)))(CONNECT_DATA=(INSTANCE_NAME=DP9)(SERVICE_NAME=ORACLE9)))";

            var conn = new OracleConnection(connectionString);
            return conn;
        }
    }

    [Serializable]
    public class FarmaciaContextException : Exception
    {
        public FarmaciaContextException()
        {
        }
    }
}