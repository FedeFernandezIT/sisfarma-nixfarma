using Sisfarma.Sincronizador.Core.Config;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.IO;
using System.Linq;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Data
{
    public class FarmaciaContext : DbContext
    {
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
        private static string _path = "";
        private static string _password = "";

        public static int ListaDeArticulo { get; set; }

        public static void Setup(string path, string password, int listaDeArticulo)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new System.ArgumentException("message", nameof(path));

            _path = path;
            _password = password ?? throw new System.ArgumentNullException(nameof(password));

            ListaDeArticulo = listaDeArticulo;
        }

        public static FarmaciaContext Default()
            => new FarmaciaContext(
                server: "nixfarma",
                database: "nixfarma",
                username: "consu",
                password: "consu");

        //public static FarmaciaContext Ventas(int year)
        //{
        //    _anioActual = year;
        //    return Ventas();
        //}

        //public static FarmaciaContext Ventas()
        //{
        //    _historicos = GetHistoricos();

        //    if (_historicos.All(x => x > _anioActual))
        //        throw new FarmaciaContextException();

        //    if (_historicos.Contains(_anioActual))
        //    {
        //        return new FarmaciaContext(
        //            server: _server,
        //            database: $@"{_path}\Hst{_anioActual}.accdb",
        //            username: _username,
        //            password: _password);
        //    }

        //    return new FarmaciaContext(
        //        server: _server,
        //        database: $@"{_path}\Ventas.accdb",
        //        username: _username,
        //        password: _password);
        //}

        public static FarmaciaContext VentasByYear(int year)
        {
            var historicos = GetHistoricos();

            if (historicos.All(x => x > year))
                throw new FarmaciaContextException();

            if (_historicos.Contains(year))
            {
                return new FarmaciaContext(
                    server: _server,
                    database: $@"{_path}\Hst{year}.accdb",
                    username: _username,
                    password: _password);
            }

            return new FarmaciaContext(
                server: _server,
                database: $@"{_path}\Ventas.accdb",
                username: _username,
                password: _password);
        }

        private static ICollection<int> GetHistoricos()
        {
            if (_historicos == null)
            {
                var historicos = Directory.GetFiles(
                path: $@"{_path}",
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
                database: $@"{_path}\Clientes.accdb",
                username: _username,
                password: _password);
        }

        public static FarmaciaContext Fidelizacion()
        {
            return new FarmaciaContext(
                server: _server,
                database: $@"{_path}\Fidelizacion.accdb",
                username: _username,
                password: _password);
        }

        public static FarmaciaContext Vendedor()
        {
            return new FarmaciaContext(
                server: _server,
                database: $@"{_path}\Vendedor.accdb",
                username: _username,
                password: _password);
        }

        public static FarmaciaContext Farmacos()
        {
            return new FarmaciaContext(
                server: _server,
                database: $@"{_path}\Farmacos.accdb",
                username: _username,
                password: _password);
        }

        public static FarmaciaContext Recepcion()
        {
            return new FarmaciaContext(
                server: _server,
                database: $@"{_path}\FarmaDen.accdb",
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
                    database: $@"{_path}\Hst{year}.accdb",
                    username: _username,
                    password: _password);
            }

            return new FarmaciaContext(
                server: _server,
                database: $@"{_path}\FarmaDen.accdb",
                username: _username,
                password: _password);
        }

        public static FarmaciaContext Proveedores()
        {
            return new FarmaciaContext(
                server: _server,
                database: $@"{_path}\Proveedo.accdb",
                username: _username,
                password: _password);
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