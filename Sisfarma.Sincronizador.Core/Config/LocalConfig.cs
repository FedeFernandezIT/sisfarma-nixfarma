using System;

namespace Sisfarma.Sincronizador.Core.Config
{
    public sealed class LocalConfig : UserConfig
    {
        private static LocalConfig _singleton = null;
        
        public string Server { get; private set; }

        public string Database { get; private set; }

        public int ListaDeCompras { get; private set; }

        private LocalConfig(string server, string database, string username, string password, int listaDeCompras)
            : base(username, password)
        {
            Server = server ?? throw new ArgumentNullException(nameof(server));
            Database = database ?? throw new ArgumentNullException(nameof(database));
            ListaDeCompras = listaDeCompras;
        }

        public static void Setup(string server, string database, string username, string password, int listaDeCompras)            
        {                        
            _singleton = null;
            _singleton = new LocalConfig(server, database, username, password, listaDeCompras);
        }

        public static void Setup(ConexionLocal conexion)
            => Setup(conexion.localServer, conexion.localBase, conexion.localUser, conexion.localPass, conexion.marketCodeList);

        public static LocalConfig GetSingletonInstance()
        {
            if (_singleton == null)
                throw new InvalidOperationException(nameof(RemoteConfig));

            return _singleton;
        }
    }
}
