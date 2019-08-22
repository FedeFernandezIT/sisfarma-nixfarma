using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Data;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia.DTO;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia
{
    public interface IVentasPremiumRepository
    {
        VentaPremiun GetOneOrDefaultByTarjeta(string tarjeta);

        VentaPremiun GetOneOrDefaultByClienteId(long id);
    }

    public class VentasPremiumRepository : FarmaciaRepository, IVentasPremiumRepository
    {
        public VentasPremiumRepository(LocalConfig config) 
            : base(config)
        { }

        public VentasPremiumRepository() { }

        public VentaPremiun GetOneOrDefaultByClienteId(long id)
        {
            var idInteger = (int)id;

            using (var db = FarmaciaContext.Fidelizacion())
            {
                var sql = $"SELECT TOP 1  PuntosIniciales, PuntosVentas, PuntosACanjear FROM Ventas_FarmaPremium WHERE ClienteUW = @id ORDER BY Fecha DESC";
                return db.Database.SqlQuery<VentaPremiun>(sql,
                        new OleDbParameter("id", idInteger))
                        .FirstOrDefault();
            }
        }

        public VentaPremiun GetOneOrDefaultByTarjeta(string tarjeta)
        {
            using (var db = FarmaciaContext.Fidelizacion())
            {
                var sql = $"SELECT TOP 1  PuntosIniciales, PuntosVentas, PuntosACanjear FROM Ventas_FarmaPremium WHERE ClienteFarma = @tarjeta ORDER BY Fecha DESC";
                return db.Database.SqlQuery<VentaPremiun>(sql,
                        new OleDbParameter("tarjeta", tarjeta))
                        .FirstOrDefault();
            }            
        }
    }
}
