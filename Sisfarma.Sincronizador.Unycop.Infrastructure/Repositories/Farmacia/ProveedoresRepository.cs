using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public class ProveedoresRepository : FarmaciaRepository, IProveedorRepository
    {
        private readonly IRecepcionRespository _recepcionRespository;

        public ProveedoresRepository(LocalConfig config,
            IRecepcionRespository recepcionRespository) : base(config)
        {
            _recepcionRespository = recepcionRespository ?? throw new System.ArgumentNullException(nameof(recepcionRespository));
        }

        public ProveedoresRepository(IRecepcionRespository recepcionRespository)
        {
            _recepcionRespository = recepcionRespository ?? throw new System.ArgumentNullException(nameof(recepcionRespository));
        }

        public Proveedor GetOneOrDefaultById(long id)
        {
            var conn = FarmaciaContext.GetConnection();
            try
            {
                var sqlExtra = string.Empty;
                var sql = $@"
                    select * from appul.ad_proveedores where codigo = {id}";

                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                if (reader.Read())
                {
                    var rNombreAb = Convert.ToString(reader["NOMBRE_AB"]);

                    return new Proveedor
                    {
                        Id = id,
                        Nombre = rNombreAb
                    };
                }

                return null;
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        public Proveedor GetOneOrDefaultByCodigoNacional(string codigoNacional)
        {
            var conn = FarmaciaContext.GetConnection();
            try
            {
                conn.Open();
                var sql = $@"SELECT PROVEEDOR
                    FROM (
                        SELECT PROVEEDOR FROM appul.ad_rec_linped
                            WHERE cant_servida <> 0 AND art_codigo = '{codigoNacional}'
                        ORDER BY fecha_recepcion DESC)
                    WHERE ROWNUM <= 1";

                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                if (reader.Read() && !Convert.IsDBNull(reader["PROVEEDOR"]))
                {
                    var id = Convert.ToInt64(reader["PROVEEDOR"]);
                    sql = $@"SELECT NOMBRE_AB from appul.ad_proveedores where codigo = {id}";
                    cmd.CommandText = sql;
                    reader = cmd.ExecuteReader();
                    if (reader.Read())
                    {
                        var nombre = Convert.ToString(reader["NOMBRE_AB"]);
                        return new Proveedor
                        {
                            Id = id,
                            Nombre = nombre
                        };
                    }
                }

                return new Proveedor { Nombre = string.Empty };
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        public IEnumerable<Proveedor> GetAll()
        {
            var conn = FarmaciaContext.GetConnection();
            var proveedores = new List<Proveedor>();
            try
            {
                conn.Open();
                var sql = $@"select codigo, nombre_ab from appul.ad_proveedores";
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var rCodigo = Convert.ToInt64(reader["codigo"]);
                    var rNombreAb = Convert.ToString(reader["nombre_ab"]);
                    proveedores.Add(new Proveedor { Id = rCodigo, Nombre = rNombreAb });
                }

                return proveedores;
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }
    }
}