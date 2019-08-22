using Sisfarma.Sincronizador.Config;
using Sisfarma.Sincronizador.Farmatic.Models;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Farmatic.Repositories
{
    public class ProveedoresRepository : FarmaticRepository
    {
        public ProveedoresRepository(FarmaticContext ctx) : base(ctx)
        { }

        public ProveedoresRepository(LocalConfig config) : base(config)
        { }

        public Proveedor GetOneOrDefault(string id)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = "SELECT * FROM Proveedor WHERE IDProveedor = @id";
                return db.Database.SqlQuery<Proveedor>(sql,
                    new SqlParameter("id", id))
                    .FirstOrDefault();
            }
        }

        public Proveedor GetOneOrDefaultByCodigoNacional(string codigoNacional)
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var join = "SELECT TOP 1 r.XProv_IdProveedor FROM Recep r " +
                    "INNER JOIN LineaRecep lr ON lr.IdRecepcion = r.IdRecepcion " +
                    "WHERE lr.XArt_IdArticu = @codigoNacional ORDER BY " +
                    "r.Hora DESC";
                var proveedorId = db.Database.SqlQuery<string>(join,
                    new SqlParameter("codigoNacional", codigoNacional))
                    .FirstOrDefault();

                if (proveedorId == null)
                    return null;

                var sql = "SELECT * FROM Proveedor WHERE IDProveedor = @id";
                return db.Database.SqlQuery<Proveedor>(sql,
                    new SqlParameter("id", proveedorId))
                    .FirstOrDefault();
            }
        }

        public IEnumerable<Proveedor> GetAll()
        {
            using (var db = FarmaticContext.Create(_config))
            {
                var sql = @"SELECT * FROM proveedor";
                return db.Database.SqlQuery<Proveedor>(sql)
                    .ToList();
            }
        }
    }
}