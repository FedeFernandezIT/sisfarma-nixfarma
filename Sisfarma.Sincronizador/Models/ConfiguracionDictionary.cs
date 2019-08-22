using System;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Models
{
    public class ConfiguracionDictionary : Dictionary<string, string>
    {
        public ConfiguracionDictionary(IDictionary<string, string> dictionary)
            : base(dictionary)
        { }

        public new string this[string campo] => TryGetValue(campo, out string valor)
            ? valor : string.Empty;

        public T GetValue<T>(string campo)
        {
            var value = this[campo];

            try
            {
                return (T)Convert.ChangeType(value, typeof(T));
            }
            catch (Exception)
            {
                return default(T);
            }
        }
    }
}