using System;

namespace Sisfarma.Sincronizador.Config
{
    public class RemoteConfig
    {
        private static RemoteConfig _singleton = null;
        
        public string Server { get; private set; }

        public string Token { get; private set; }

        private RemoteConfig(string server, string token)
        {
            Server = server ?? throw new ArgumentNullException(nameof(server));
            Token = token ?? throw new ArgumentNullException(nameof(token));
        }            
            

        public static void Setup(string server, string token)
        {
            _singleton = null;
            _singleton = new RemoteConfig(server, token);                        
        }

        public static RemoteConfig GetSingletonInstance()
        {
            if (_singleton == null)
                throw new InvalidOperationException(nameof(RemoteConfig));

            return _singleton;
        }
    }
}
