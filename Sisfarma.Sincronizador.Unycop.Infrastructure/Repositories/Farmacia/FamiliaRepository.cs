using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System;
using System.Collections.Generic;

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

                reader.Close();
                reader.Dispose();

                return familias;
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

        public IEnumerable<Familia> GetByDescripcion()
        {
            var conn = FarmaciaContext.GetConnection();
            var familias = new List<Familia>();
            try
            {
                conn.Open();
                var sql = $@"select * from appul.ab_subfamilias WHERE descripcion NOT IN ('ESPECIALIDAD', 'EFP', 'SIN FAMILIA') AND descripcion NOT LIKE '%ESPECIALIDADES%' AND descripcion NOT LIKE '%Medicamento%'";
                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var rDescripcion = Convert.ToString(reader["DESCRIPCION"]);
                    var rFamCodigo = Convert.ToString(reader["FAM_CODIGO"]);
                    var rFamEmpCodigo = Convert.ToString(reader["FAM_EMP_CODIGO"]);

                    sql = $@"select * from appul.ab_familias WHERE codigo = '{rFamCodigo}' AND emp_codigo = '{rFamEmpCodigo}'";
                    cmd = conn.CreateCommand();
                    cmd.CommandText = sql;
                    var readerFamilia = cmd.ExecuteReader();

                    var padre = readerFamilia.Read() ? Convert.ToString(readerFamilia["DESCRIPCION"]) : "<SIN PADRE>";

                    familias.Add(new Familia
                    {
                        Nombre = rDescripcion,
                        Padre = padre
                    });
                }

                reader.Close();
                reader.Dispose();

                return familias;
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

                reader.Close();
                reader.Dispose();
                return new Familia { Nombre = descripcion };
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

                reader.Close();
                reader.Dispose();
                return new Familia { Nombre = descripcion };
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

                reader.Close();
                reader.Dispose();

                return familias;
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