using Oracle.DataAccess.Client;
using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public class FamiliaRepository : FarmaciaRepository, IFamiliaRepository
    {
        public FamiliaRepository(LocalConfig config) : base(config)
        { }

        public FamiliaRepository()
        {
        }

        public IEnumerable<Familia> GetAll()
        {
            var conn = FarmaciaContext.GetConnection();
            var familias = new List<Familia>();
            try
            {
                conn.Open();
                var sql = $@"select * from appul.ab_familias";
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var descripcion = Convert.ToString(reader["DESCRIPCION"]) ?? string.Empty;
                    familias.Add(new Familia { Nombre = descripcion });
                }

                return familias;
            }
            catch (Exception ex)
            {
                return null;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        public IEnumerable<Familia> GetByDescripcion()
        {
            using (var db = FarmaciaContext.Default())
            {
                var sql = @"select nombre from familias WHERE nombre NOT IN ('ESPECIALIDAD', 'EFP', 'SIN FAMILIA') AND nombre NOT LIKE '%ESPECIALIDADES%' AND nombre NOT LIKE '%Medicamento%'";
                return db.Database.SqlQuery<Familia>(sql)
                    .ToList();
            }
        }

        public Familia GetOneOrDefaultById(long id)
        {
            var conn = FarmaciaContext.GetConnection();

            try
            {
                conn.Open();
                var sql = $@"select DESCRIPCION from appul.ab_familias where codigo={id}";
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                var descripcion = string.Empty;
                if (reader.Read())
                    descripcion = Convert.ToString(reader["DESCRIPCION"]) ?? string.Empty;

                return new Familia { Nombre = descripcion };
            }
            catch (Exception ex)
            {
                return null;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        public string GetSuperFamiliaDescripcionByFamilia(string familia)
        {
            throw new NotImplementedException();
        }

        public string GetSuperFamiliaDescripcionById(short familia)
        {
            throw new NotImplementedException();
        }

        public string GetSuperFamiliaDescripcionById(string id)
        {
            throw new NotImplementedException();
        }

        public Familia GetSubFamiliaOneOrDefault(long familia, string subFamilia)
        {
            var conn = FarmaciaContext.GetConnection();

            try
            {
                conn.Open();
                var sql = $@"select DESCRIPCION from appul.ab_subfamilias
                    where fam_codigo = {familia} AND codigo = '{subFamilia}'";
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                var descripcion = string.Empty;
                if (reader.Read())
                    descripcion = Convert.ToString(reader["DESCRIPCION"]) ?? string.Empty;

                return new Familia { Nombre = descripcion };
            }
            catch (Exception ex)
            {
                return null;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        public IEnumerable<Familia> GetAllSubFamilias()
        {
            var conn = FarmaciaContext.GetConnection();
            var familias = new List<Familia>();
            try
            {
                conn.Open();
                var sql = $@"select * from appul.ab_subfamilias";
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var descripcion = Convert.ToString(reader["DESCRIPCION"]) ?? string.Empty;
                    familias.Add(new Familia { Nombre = descripcion });
                }

                return familias;
            }
            catch (Exception ex)
            {
                return null;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }
    }
}