using System;
using System.Collections.Generic;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes.DTO;
using FAR = Sisfarma.Sincronizador.Domain.Entities.Farmacia;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IClientesExternalService : IClientesExternalServiceNew
    {
        bool AnyWithDni(string dni);
        string GetDniTrackingLast();
        void Insert(string trabajador, string tarjeta, string idCliente, string nombre, string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento, string sexo, string tipo, DateTime? fechaAlta, int baja, int lopd, bool withTrack = false);
        void InsertOrUpdate(string trabajador, string tarjeta, string idCliente, string nombre, string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento, string sexo, DateTime? fechaAlta, int baja, int lopd, bool esHueco = false);
        void InsertOrUpdate(string trabajador, string tarjeta, string idCliente, string dniCliente, string nombre, string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento, string sexo, DateTime? fechaAlta, int baja, int lopd, bool esHueco = false);
        void InsertOrUpdate(string trabajador, string tarjeta, string idCliente, string dniCliente, string nombre, string telefono, string direccion, string movil, string email, long fechaNacimiento, string sexo, DateTime? fechaAlta, int baja, int lopd, bool esHueco = false);
        void InsertOrUpdateBeBlue(string trabajador, string tarjeta, string idCliente, string dniCliente, string nombre, string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento, string sexo, DateTime? fechaAlta, int baja, int lopd, int esBeBlue, bool esHueco = false);
        void InsertOrUpdateBeBlue(string trabajador, string tarjeta, string idCliente, string dniCliente, string nombre, string telefono, string direccion, string movil, string email, long fechaNacimiento, string sexo, DateTime? fechaAlta, int baja, int lopd, int esBeBlue, bool esHueco = false);
        void ResetDniTracking();
        void Update(string trabajador, string tarjeta, string nombre, string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento, string sexo, DateTime? fechaAlta, int baja, int lopd, string idCliente, bool withTrack = false);
        void UpdatePuntos(UpdatePuntaje pp);
    }

    public interface IClientesExternalServiceNew
    {
        void InsertOrUpdate(Cliente cliente);

        void Sincronizar(IEnumerable<FAR.Cliente> clientes);        

        void SincronizarHueco(FAR.Cliente cliente, bool cargarPuntos = false);

        void SincronizarHueco(FAR.Cliente cliente, bool beBlue, bool cargarPuntos = false);
    }
}