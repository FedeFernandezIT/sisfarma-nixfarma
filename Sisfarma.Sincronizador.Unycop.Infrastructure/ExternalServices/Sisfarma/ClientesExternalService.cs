using Sisfarma.RestClient;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes.DTO;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using FAR = Sisfarma.Sincronizador.Domain.Entities.Farmacia;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.ExternalServices.Sisfarma
{
    public class ClientesExternalService : FisiotesExternalService, IClientesExternalService, IClientesExternalServiceNew
    {
        public ClientesExternalService(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        { }

        public bool AnyWithDni(string dni)
        {
            throw new NotImplementedException();
        }

        public string GetDniTrackingLast()
        {
            throw new NotImplementedException();
        }

        public void Insert(string trabajador, string tarjeta, string idCliente, string nombre, string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento, string sexo, string tipo, DateTime? fechaAlta, int baja, int lopd, bool withTrack = false)
        {
            throw new NotImplementedException();
        }

        public void InsertOrUpdate(string trabajador, string tarjeta, string idCliente, string nombre, string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento, string sexo, DateTime? fechaAlta, int baja, int lopd, bool esHueco = false)
        {
            throw new NotImplementedException();
        }

        public void InsertOrUpdate(string trabajador, string tarjeta, string idCliente, string dniCliente, string nombre, string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento, string sexo, DateTime? fechaAlta, int baja, int lopd, bool esHueco = false)
        {
            throw new NotImplementedException();
        }

        public void InsertOrUpdate(string trabajador, string tarjeta, string idCliente, string dniCliente, string nombre, string telefono, string direccion, string movil, string email, long fechaNacimiento, string sexo, DateTime? fechaAlta, int baja, int lopd, bool esHueco = false)
        {
            throw new NotImplementedException();
        }

        public void InsertOrUpdate(Cliente cliente)
        {
            throw new NotImplementedException();
        }

        public void InsertOrUpdateBeBlue(string trabajador, string tarjeta, string idCliente, string dniCliente, string nombre, string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento, string sexo, DateTime? fechaAlta, int baja, int lopd, int esBeBlue, bool esHueco = false)
        {
            throw new NotImplementedException();
        }

        public void InsertOrUpdateBeBlue(string trabajador, string tarjeta, string idCliente, string dniCliente, string nombre, string telefono, string direccion, string movil, string email, long fechaNacimiento, string sexo, DateTime? fechaAlta, int baja, int lopd, int esBeBlue, bool esHueco = false)
        {
            throw new NotImplementedException();
        }

        public void ResetDniTracking()
        {
            _restClient
                .Resource(_config.Clientes.ResetDniTracking)
                .SendPut();
        }

        public void Sincronizar(IEnumerable<FAR.Cliente> clientes)
        {
            var resource = _config.Clientes.InsertBulk;
            var bulk = clientes.Select(cc => GenerarClienteDinamico(cc));

            _restClient
            .Resource(resource)
            .SendPost(new
            {
                bulk = bulk
            });
        }

        public object GenerarClienteDinamico(FAR.Cliente cliente)
        {
            if (cliente.BeBlue.HasValue)
            {
                return cliente.DebeCargarPuntos
                    ? GenerarAnonymousClientePuntuado(cliente, cliente.BeBlue.Value)
                    : GenerarAnonymousClienteSinPuntuar(cliente, cliente.BeBlue.Value);
            }

            return cliente.DebeCargarPuntos
                ? GenerarAnonymousClientePuntuado(cliente)
                : GenerarAnonymousClienteSinPuntuar(cliente);
        }

        public void Sincronizar(FAR.Cliente cliente, bool beBlue, bool cargarPuntos = false)
        {
            var resource = _config.Clientes.Insert.Replace("{dni}", $"{cliente.Id}");
            Sincronizar(cliente, beBlue, cargarPuntos, resource);
        }

        public void SincronizarHueco(FAR.Cliente cliente, bool cargarPuntos = false)
        {
            var resource = _config.Clientes.InsertHueco.Replace("{dni}", $"{cliente.Id}");
            Sincronizar(cliente, cargarPuntos, resource);
        }

        public void SincronizarHueco(FAR.Cliente cliente, bool beBlue, bool cargarPuntos = false)
        {
            var resource = _config.Clientes.InsertHueco.Replace("{dni}", $"{cliente.Id}");
            Sincronizar(cliente, beBlue, cargarPuntos, resource);
        }

        private void Sincronizar(FAR.Cliente cliente, bool cargarPuntos, string resource)
        {
            var clienteToSend = (cargarPuntos) ?
                GenerarAnonymousClientePuntuado(cliente) :
                GenerarAnonymousClienteSinPuntuar(cliente);

            _restClient
                .Resource(resource)
                .SendPut(clienteToSend);
        }

        private void Sincronizar(FAR.Cliente cliente, bool beBlue, bool cargarPuntos, string resource)
        {
            var clienteToSend = (cargarPuntos) ?
                GenerarAnonymousClientePuntuado(cliente, beBlue) :
                GenerarAnonymousClienteSinPuntuar(cliente, beBlue);

            _restClient
                .Resource(resource)
                .SendPut(clienteToSend);
        }

        private object GenerarAnonymousClientePuntuado(FAR.Cliente cliente)
        {
            return new
            {
                nombre_tra = cliente.Trabajador,
                dni_tra = "0",
                tarjeta = cliente.Tarjeta,
                dniCliente = cliente.NumeroIdentificacion,
                apellidos = cliente.NombreCompleto.Strip(),
                telefono = cliente.Telefono,
                direccion = cliente.Direccion.Strip(),
                movil = cliente.Celular,
                email = cliente.Email,
                fecha_nacimiento = cliente.FechaNacimiento.ToDateInteger(),
                puntos = cliente.Puntos,
                sexo = cliente.Sexo,
                tipo = cliente.Tipo,
                fechaAlta = cliente.FechaAlta.ToIsoString(),
                baja = cliente.Baja.ToInteger(),
                estado_civil = cliente.EstadoCivil,
                lopd = cliente.LOPD.ToInteger()
            };
        }

        private object GenerarAnonymousClientePuntuado(FAR.Cliente cliente, bool beBlue)
        {
            return new
            {
                nombre_tra = cliente.Trabajador,
                dni_tra = "0",
                tarjeta = cliente.Tarjeta,
                dniCliente = cliente.NumeroIdentificacion,
                apellidos = cliente.NombreCompleto.Strip(),
                telefono = cliente.Telefono,
                direccion = cliente.Direccion.Strip(),
                movil = cliente.Celular,
                email = cliente.Email,
                fecha_nacimiento = cliente.FechaNacimiento.ToDateInteger(),
                puntos = cliente.Puntos,
                sexo = cliente.Sexo,
                tipo = cliente.Tipo,
                fechaAlta = cliente.FechaAlta.ToIsoString(),
                baja = cliente.Baja.ToInteger(),
                estado_civil = cliente.EstadoCivil,
                lopd = cliente.LOPD.ToInteger(),
                beBlue = beBlue.ToInteger()
            };
        }

        private object GenerarAnonymousClienteSinPuntuar(FAR.Cliente cliente)
        {
            return new
            {
                nombre_tra = cliente.Trabajador,
                dni_tra = "0",
                tarjeta = cliente.Tarjeta,
                dniCliente = cliente.NumeroIdentificacion,
                apellidos = cliente.NombreCompleto.Strip(),
                telefono = cliente.Telefono,
                direccion = cliente.Direccion.Strip(),
                movil = cliente.Celular,
                email = cliente.Email,
                fecha_nacimiento = cliente.FechaNacimiento.ToDateInteger(),
                sexo = cliente.Sexo,
                tipo = cliente.Tipo,
                fechaAlta = cliente.FechaAlta.ToIsoString(),
                baja = cliente.Baja.ToInteger(),
                estado_civil = cliente.EstadoCivil,
                lopd = cliente.LOPD.ToInteger()
            };
        }

        private object GenerarAnonymousClienteSinPuntuar(FAR.Cliente cliente, bool beBlue)
        {
            return new
            {
                nombre_tra = cliente.Trabajador,
                dni_tra = "0",
                tarjeta = cliente.Tarjeta,
                dniCliente = cliente.NumeroIdentificacion,
                apellidos = cliente.NombreCompleto.Strip(),
                telefono = cliente.Telefono,
                direccion = cliente.Direccion.Strip(),
                movil = cliente.Celular,
                email = cliente.Email,
                fecha_nacimiento = cliente.FechaNacimiento.ToDateInteger(),
                sexo = cliente.Sexo,
                tipo = cliente.Tipo,
                fechaAlta = cliente.FechaAlta.ToIsoString(),
                baja = cliente.Baja.ToInteger(),
                estado_civil = cliente.EstadoCivil,
                lopd = cliente.LOPD.ToInteger(),
                beBlue = beBlue.ToInteger()
            };
        }

        public void Update(string trabajador, string tarjeta, string nombre, string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento, string sexo, DateTime? fechaAlta, int baja, int lopd, string idCliente, bool withTrack = false)
        {
            throw new NotImplementedException();
        }

        public void UpdatePuntos(UpdatePuntaje pp)
        {
            throw new NotImplementedException();
        }
    }
}