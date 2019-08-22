using System;

namespace Sisfarma.Sincronizador.Config
{
    public abstract class UserConfig
    {        
        public string Username { get; protected set; }

        public string Password { get; protected set; }

        protected UserConfig(string username, string password)
        {
            Username = username ?? throw new ArgumentNullException(nameof(username));
            Password = password ?? throw new ArgumentNullException(nameof(password));
        }             
    }
}
