using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes.DTO;
using System;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class ClientesExternalService : FisiotesExternalService
    {        
        public ClientesExternalService(IRestClient restClient, FisiotesConfig config) : base(restClient, config)
        { }

        public string GetDniTrackingLast()
        {
            return _restClient
                .Resource(_config.Clientes.GetDniTrackingLast)
                .SendGet<Cliente>().dni;
        }

        public void ResetDniTracking()
        {
            _restClient
                .Resource(_config.Clientes.ResetDniTracking)
                .SendPut();
        }

        public bool AnyWithDni(string dni)
        {
            try
            {
                _restClient
                .Resource(_config.Clientes.GetByDni.Replace("{dni}", $"{dni}"))
                .SendGet();
                return true;
            }
            catch (RestClientNotFoundException)
            {
                return false;
            }
        }

        public void InsertOrUpdate(string trabajador, string tarjeta, string idCliente, string nombre,
            string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento,
            string sexo, DateTime? fechaAlta, int baja, int lopd, bool esHueco = false)
        {
            var resource = (esHueco)
                ? _config.Clientes.InsertHueco.Replace("{dni}", $"{idCliente}")
                : _config.Clientes.Insert.Replace("{dni}", $"{idCliente}");

            _restClient
                .Resource(resource)
                .SendPut(new
                {
                    dni_tra = "0",
                    nombre_tra = trabajador,
                    tarjeta = tarjeta,
                    apellidos = nombre,
                    telefono = telefono,
                    direccion = direccion,
                    movil = movil,
                    email = email,
                    fecha_nacimiento = fechaNacimiento,
                    puntos = puntos,
                    sexo = sexo,
                    fechaAlta = fechaAlta.ToIsoString(),
                    baja = baja,
                    lopd = lopd
                });
        }

        public void InsertOrUpdate(string trabajador, string tarjeta, string idCliente, string dniCliente, string nombre,
            string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento,
            string sexo, DateTime? fechaAlta, int baja, int lopd, bool esHueco = false)
        {
            var resource = (esHueco)
                ? _config.Clientes.InsertHueco.Replace("{dni}", $"{idCliente}")
                : _config.Clientes.Insert.Replace("{dni}", $"{idCliente}");

            _restClient
                .Resource(resource)
                .SendPut(new
                {
                    dni_tra = "0",
                    nombre_tra = trabajador,
                    tarjeta = tarjeta,
                    apellidos = nombre,
                    telefono = telefono,
                    direccion = direccion,
                    movil = movil,
                    email = email,
                    fecha_nacimiento = fechaNacimiento,
                    puntos = puntos,
                    sexo = sexo,
                    fechaAlta = fechaAlta.ToIsoString(),
                    baja = baja,
                    lopd = lopd,
                    dniCliente = dniCliente
                });
        }

        public void InsertOrUpdate(string trabajador, string tarjeta, string idCliente, string dniCliente, string nombre,
            string telefono, string direccion, string movil, string email, long fechaNacimiento,
            string sexo, DateTime? fechaAlta, int baja, int lopd, bool esHueco = false)
        {
            var resource = (esHueco)
                ? _config.Clientes.InsertHueco.Replace("{dni}", $"{idCliente}")
                : _config.Clientes.Insert.Replace("{dni}", $"{idCliente}");

            _restClient
                .Resource(resource)
                .SendPut(new
                {
                    dni_tra = "0",
                    nombre_tra = trabajador,
                    tarjeta = tarjeta,
                    apellidos = nombre,
                    telefono = telefono,
                    direccion = direccion,
                    movil = movil,
                    email = email,
                    fecha_nacimiento = fechaNacimiento,
                    sexo = sexo,
                    fechaAlta = fechaAlta.ToIsoString(),
                    baja = baja,
                    lopd = lopd,
                    dniCliente = dniCliente
                });
        }

        public void InsertOrUpdateBeBlue(string trabajador, string tarjeta, string idCliente, string dniCliente, string nombre,
            string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento,
            string sexo, DateTime? fechaAlta, int baja, int lopd, int esBeBlue, bool esHueco = false)
        {
            var resource = (esHueco)
                ? _config.Clientes.InsertHueco.Replace("{dni}", $"{idCliente}")
                : _config.Clientes.Insert.Replace("{dni}", $"{idCliente}");

            _restClient
                .Resource(resource)
                .SendPut(new
                {
                    dni_tra = "0",
                    nombre_tra = trabajador,
                    tarjeta = tarjeta,
                    apellidos = nombre,
                    telefono = telefono,
                    direccion = direccion,
                    movil = movil,
                    email = email,
                    fecha_nacimiento = fechaNacimiento,
                    puntos = puntos,
                    sexo = sexo,
                    fechaAlta = fechaAlta.ToIsoString(),
                    baja = baja,
                    lopd = lopd,
                    dniCliente = dniCliente
                });
        }

        public void InsertOrUpdateBeBlue(string trabajador, string tarjeta, string idCliente, string dniCliente, string nombre,
            string telefono, string direccion, string movil, string email, long fechaNacimiento,
            string sexo, DateTime? fechaAlta, int baja, int lopd, int esBeBlue, bool esHueco = false)
        {
            var resource = (esHueco)
                ? _config.Clientes.InsertHueco.Replace("{dni}", $"{idCliente}")
                : _config.Clientes.Insert.Replace("{dni}", $"{idCliente}");

            _restClient
                .Resource(resource)
                .SendPut(new
                {
                    dni_tra = "0",
                    nombre_tra = trabajador,
                    tarjeta = tarjeta,
                    apellidos = nombre,
                    telefono = telefono,
                    direccion = direccion,
                    movil = movil,
                    email = email,
                    fecha_nacimiento = fechaNacimiento,
                    sexo = sexo,
                    fechaAlta = fechaAlta.ToIsoString(),
                    baja = baja,
                    lopd = lopd,
                    dniCliente = dniCliente
                });
        }

        public void Insert(string trabajador, string tarjeta, string idCliente, string nombre,
            string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento,
            string sexo, string tipo, DateTime? fechaAlta, int baja, int lopd, bool withTrack = false)
        {
            _restClient
                .Resource(_config.Clientes.Insert.Replace("{dni}", $"{idCliente}"))
                .SendPost(new Cliente
                {
                    apellidos = nombre,
                    telefono = telefono,
                    direccion = direccion,
                    movil = movil,
                    email = email,
                    fecha_nacimiento = fechaNacimiento
                });
        }

        public void Update(string trabajador, string tarjeta, string nombre, string telefono, string direccion,
            string movil, string email, decimal puntos, long fechaNacimiento, string sexo, DateTime? fechaAlta, int baja, int lopd, string idCliente,
            bool withTrack = false)
        {
            var clientes = new[] { new
            {
                dni = idCliente,
                nombre = nombre,
                apellidos = nombre,
                telefono = telefono
            } };

            _restClient
                .Resource(_config.Clientes.Update)
                .SendPut(new
                {
                    bulk = clientes
                });
        }

        public void UpdatePuntos(UpdatePuntaje pp)
        {
            var clientes = new[] { new
            {
                dni = pp.dni,
                puntos = pp.puntos
            } };

            _restClient
                .Resource(_config.Clientes.Update)
                .SendPut(new
                {
                    bulk = clientes
                });
        }       
    }
}