using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Fisiotes.DTO.Clientes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Fisiotes.Repositories
{
    public class ClientesRepository : FisiotesRepository
    {
        public ClientesRepository(FisiotesContext ctx) : base(ctx)
        {
        }

        public ClientesRepository(IRestClient restClient, FisiotesConfig config) : base(restClient, config)
        {
        }

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

        public void CheckAndCreateFields()
        {
            const string table = @"SELECT * from clientes LIMIT 0,1;";
            var fields = new[] { "baja", "fechaAlta" };
            var alters = new[]
            {
                @"ALTER TABLE clientes ADD `baja` tinyint(1) DEFAULT 0 AFTER dia_alta;",
                @"ALTER TABLE clientes ADD `fechaAlta` datetime AFTER dia_alta;"
            };

            CheckAndCreateFieldsTemplate(table, fields, alters);
        }

        #region SQL Methods

        public void ResetDniTrackingSql()
        {
            var sql = "UPDATE clientes SET dni_tra = 0";
            _ctx.Database.ExecuteSqlCommand(sql);
        }

        public string GetDniTrackingLastSql()
        {
            var sql = @"SELECT dni FROM clientes WHERE dni_tra = 1";
            return _ctx.Database.SqlQuery<string>(sql)
                .FirstOrDefault() ?? "0";
        }

        public bool AnyWithDniSql(string filter)
        {
            var sql = @"SELECT * FROM clientes WHERE dni = @dni";
            return _ctx.Database.SqlQuery<Cliente>(sql,
                new SqlParameter("dni", filter))
                .Any();
        }

        public void InsertSql(string trabajador, string tarjeta, string idCliente, string nombre,
            string telefono, string direccion, string movil, string email, decimal puntos, long fechaNacimiento,
            string sexo, string tipo, DateTime? fechaAlta, int baja, int lopd, bool withTrack = false)
        {
            try
            {
                var sql = string.Empty;
                if (withTrack)
                    sql = "INSERT IGNORE INTO clientes (" +
                            "dni_tra, nombre_tra, tarjeta, dni, apellidos, telefono, direccion, movil, email, puntos, fecha_nacimiento, sexo, tipo, fechaAlta, baja, lopd) " +
                            "VALUES(" +
                            "'1', @trabajador, @tarjeta, @idCliente, @nombre, @telefono, @direccion, @movil, @email, @puntos, @fechaNacimiento, @sexo, @tipo, @fechaAlta, @baja, @lopd)";
                else
                    sql = "INSERT IGNORE INTO clientes (" +
                            "nombre_tra, tarjeta, dni, apellidos, telefono, direccion, movil, email, puntos, fecha_nacimiento, sexo, tipo, fechaAlta, baja, lopd) " +
                            "VALUES(" +
                            "@trabajador, @tarjeta, @idCliente, @nombre, @telefono, @direccion, @movil, @email, @puntos, @fechaNacimiento, @sexo, @tipo, @fechaAlta, @baja, @lopd)";

                _ctx.Database.ExecuteSqlCommand(sql,
                        new SqlParameter("trabajador", trabajador),
                        new SqlParameter("tarjeta", tarjeta),
                        new SqlParameter("idCliente", idCliente),
                        new SqlParameter("nombre", nombre),
                        new SqlParameter("telefono", telefono),
                        new SqlParameter("direccion", direccion),
                        new SqlParameter("movil", movil),
                        new SqlParameter("email", email),
                        new SqlParameter("puntos", puntos),
                        new SqlParameter("fechaNacimiento", fechaNacimiento),
                        new SqlParameter("sexo", sexo),
                        new SqlParameter("tipo", tipo),
                        new SqlParameter("fechaAlta", fechaAlta),
                        new SqlParameter("baja", baja),
                        new SqlParameter("lopd", lopd));
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public void UpdateSql(string trabajador, string tarjeta, string nombre, string telefono, string direccion,
            string movil, string email, decimal puntos, long fechaNacimiento, string sexo, DateTime? fechaAlta, int baja, int lopd, string idCliente,
            bool withTrack = false)
        {
            var sql = string.Empty;
            if (withTrack)
                sql = "UPDATE IGNORE clientes SET dni_tra = '1', nombre_tra = @trabajador, tarjeta = @tarjeta, apellidos = @nombre, " +
                        "telefono = @telefono, direccion = @direccion, movil = @movil, email = @email, puntos = @puntos, fecha_nacimiento = @fechaNacimiento, " +
                        "sexo = @sexo, fechaAlta = @fechaAlta, baja = @baja, lopd = @lopd " +
                        "WHERE dni = @idCliente";
            else
                sql = "UPDATE IGNORE clientes SET nombre_tra = @trabajador, tarjeta = @tarjeta, apellidos = @nombre, " +
                        "telefono = @telefono, direccion = @direccion, movil = @movil, email = @email, puntos = @puntos, fecha_nacimiento = @fechaNacimiento, " +
                        "sexo = @sexo, fechaAlta = @fechaAlta, baja = @baja, lopd = @lopd " +
                        "WHERE dni = @idCliente";

            _ctx.Database.ExecuteSqlCommand(sql,
                    new SqlParameter("trabajador", trabajador),
                    new SqlParameter("tarjeta", tarjeta),
                    new SqlParameter("nombre", nombre),
                    new SqlParameter("telefono", telefono),
                    new SqlParameter("direccion", direccion),
                    new SqlParameter("movil", movil),
                    new SqlParameter("email", email),
                    new SqlParameter("puntos", puntos),
                    new SqlParameter("fechaNacimiento", fechaNacimiento),
                    new SqlParameter("sexo", sexo),
                    new SqlParameter("fechaAlta", fechaAlta),
                    new SqlParameter("baja", baja),
                    new SqlParameter("lopd", lopd),
                    new SqlParameter("idCliente", idCliente));
        }

        #endregion SQL Methods
    }
}