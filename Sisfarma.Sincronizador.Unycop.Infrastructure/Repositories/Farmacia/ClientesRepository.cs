using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Farmatic.Models;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia
{
    public class ClientesRepository : FarmaciaRepository, IClientesRepository
    {
        private readonly IVentasPremiumRepository _ventasPremium;

        private readonly bool _premium;        

        public ClientesRepository(LocalConfig config, bool premium)
            : base(config) => _premium = premium;

        public ClientesRepository()
        {
            _premium = false;
            _ventasPremium = null;
        }
            

        public ClientesRepository(IVentasPremiumRepository ventasPremium)
        {
            _premium = true;
            _ventasPremium = ventasPremium ?? throw new ArgumentNullException(nameof(ventasPremium));
        }

        public List<Cliente> GetGreatThanId(long id)
        {
            var rs = new List<DTO.Cliente>();
            using (var db = FarmaciaContext.Clientes())
            {
                var sql = @"SELECT c.ID_Cliente as Id, c.Nombre, c.Direccion, c.Localidad, c.Cod_Postal as CodigoPostal, c.Fecha_Alta as FechaAlta, c.Fecha_Baja as Baja, c.Sexo, c.ControlLOPD as LOPD, c.DNI_CIF as DNICIF, c.Telefono, c.Fecha_Nac as FechaNacimiento, c.Movil, c.Correo, c.Clave as Tarjeta, c.Puntos, ec.nombre AS EstadoCivil FROM clientes c LEFT JOIN estadoCivil ec ON ec.id = c.estadoCivil WHERE Id_cliente > @id ORDER BY Id_cliente";
                rs = db.Database.SqlQuery<DTO.Cliente>(sql,
                    new OleDbParameter("id", (int) id))
                    //.Take(1000)
                    .ToList();                    
            }
                        
            return rs.Select(GenerateCliente).ToList();
        }


        public List<DTO.Cliente> GetGreatThanIdAsDTO(long id)
        {            
            using (var db = FarmaciaContext.Clientes())
            {
                var sql = @"SELECT c.ID_Cliente as Id, c.Nombre, c.Direccion, c.Localidad, c.Cod_Postal as CodigoPostal, c.Fecha_Alta as FechaAlta, c.Fecha_Baja as Baja, c.Sexo, c.ControlLOPD as LOPD, c.DNI_CIF as DNICIF, c.Telefono, c.Fecha_Nac as FechaNacimiento, c.Movil, c.Correo, c.Clave as Tarjeta, c.Puntos, ec.nombre AS EstadoCivil FROM clientes c LEFT JOIN estadoCivil ec ON ec.id = c.estadoCivil WHERE Id_cliente > @id ORDER BY Id_cliente";
                return db.Database.SqlQuery<DTO.Cliente>(sql,
                    new OleDbParameter("id", (int)id))                    
                    .ToList();
            }            
        }

        public T GetAuxiliarById<T>(string cliente) where T : ClienteAux
        {
            using (var db = FarmaciaContext.Clientes())
            {
                var sql = @"SELECT * FROM ClienteAux WHERE idCliente = @idCliente";
                return db.Database.SqlQuery<T>(sql,
                    new SqlParameter("idCliente", cliente))
                    .FirstOrDefault();
            }
        }

        public decimal GetTotalPuntosById(string idCliente)
        {
            using (var db = FarmaciaContext.Clientes())
            {
                var sql = @"SELECT ISNULL(SUM(cantidad), 0) AS puntos FROM HistoOferta WHERE IdCliente = @idCliente AND TipoAcumulacion = 'P'";
                return db.Database.SqlQuery<decimal>(sql,
                    new SqlParameter("idCliente", idCliente))
                    .FirstOrDefault();
            }
        }

        public bool HasSexoField()
        {
            using (var db = FarmaciaContext.Clientes())
            {
                var existFieldSexo = false;

                // Chekeamos si existen los campos
                var connection = db.Database.Connection;

                var sql = "SELECT TOP 1 * FROM ClienteAux";
                var command = connection.CreateCommand();
                command.CommandText = sql;
                connection.Open();
                var reader = command.ExecuteReader();
                var schemaTable = reader.GetSchemaTable();

                foreach (DataRow row in schemaTable.Rows)
                {
                    if (row[schemaTable.Columns["ColumnName"]].ToString()
                        .Equals("sexo", StringComparison.CurrentCultureIgnoreCase))
                    {
                        existFieldSexo = true;
                        break;
                    }
                }
                connection.Close();
                return existFieldSexo;
            }
        }

        public Cliente GetOneOrDefaultById(long id)
        {
            var idInteger = (int)id;
            DTO.Cliente dto;

            using (var db = FarmaciaContext.Clientes())
            {
                var sql = @"SELECT c.ID_Cliente as Id, c.Nombre, c.Direccion, c.Localidad, c.Cod_Postal as CodigoPostal, c.Fecha_Alta as FechaAlta, c.Fecha_Baja as Baja, c.Sexo, c.ControlLOPD as LOPD, c.DNI_CIF as DNICIF, c.Telefono, c.Fecha_Nac as FechaNacimiento, c.Movil, c.Correo, c.Clave as Tarjeta, c.Puntos, ec.nombre AS EstadoCivil FROM clientes c LEFT JOIN estadoCivil ec ON ec.id = c.estadoCivil WHERE Id_cliente = @id";
                dto = db.Database.SqlQuery<DTO.Cliente>(sql,
                    new OleDbParameter("id", idInteger))
                    .FirstOrDefault();                
            }

            if (dto == null)
                return default(Cliente);

            return GenerateCliente(dto);
        }

        private long GetPuntosPremiumByCliente(Cliente cliente)
        {
            var venta = !cliente.HasTarjeta()
                ? _ventasPremium.GetOneOrDefaultByClienteId(cliente.Id)
                : _ventasPremium.GetOneOrDefaultByTarjeta(cliente.Tarjeta)
                    ?? _ventasPremium.GetOneOrDefaultByClienteId(cliente.Id);

            return venta != null
                ? venta.PuntosIniciales + venta.PuntosVentas - venta.PuntosACanjear 
                : 0;
        }

        public bool Exists(int id)
            => GetOneOrDefaultById(id) != null;

        public bool EsBeBlue(string cliente)
        {
            var id = cliente.ToLongOrDefault();
            using (var db = FarmaciaContext.Clientes())
            {
                var sql = @"SELECT Perfil FROM Clientes WHERE ID_Cliente = @id";
                var tipo = db.Database.SqlQuery<int?>(sql,
                    new SqlParameter("id", id))
                    .FirstOrDefault();

                if (!tipo.HasValue)
                    return false;

                return tipo.Value == 2;
            }
        }

        public Cliente GenerateCliente(DTO.Cliente dto)
        {
            var cliente = new Cliente
            {
                Id = dto.Id,
                Celular = dto.Movil,
                Email = dto.Correo,
                Tarjeta = dto.Tarjeta,
                EstadoCivil = dto.EstadoCivil,
                FechaNacimiento = dto.FechaNacimiento > 0 ? (DateTime?) $"{dto.FechaNacimiento}".ToDateTimeOrDefault("yyyyMMdd") : null,
                Telefono = dto.Telefono,
                Puntos = (long)dto.Puntos,
                NumeroIdentificacion = dto.DNICIF,
                LOPD = dto.LOPD,
                Sexo = dto.Sexo.ToUpper() ==  "H" ? "HOMBRE" : dto.Sexo.ToUpper() == "M" ? "MUJER" : dto.Sexo,
                Baja = dto.Baja != 0,
                FechaAlta = $"{dto.FechaAlta}".ToDateTimeOrDefault("yyyyMMdd"),
                Direccion = dto.Direccion,
                Localidad = dto.Localidad,
                CodigoPostal = dto.CodigoPostal,
                NombreCompleto = dto.Nombre,
            };

            if (_premium)
                cliente.Puntos += GetPuntosPremiumByCliente(cliente);

            return cliente;
        }
    }
}