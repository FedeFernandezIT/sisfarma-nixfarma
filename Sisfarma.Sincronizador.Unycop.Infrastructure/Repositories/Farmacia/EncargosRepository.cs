using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia
{
    public class EncargosRepository : FarmaciaRepository, IEncargosRepository
    {
        private readonly IClientesRepository _clientesRepository;
        private readonly IProveedorRepository _proveedorRepository;
        private readonly IFarmacoRepository _farmacoRepository;
        private readonly ICategoriaRepository _categoriaRepository;
        private readonly IFamiliaRepository _familiaRepository;
        private readonly ILaboratorioRepository _laboratorioRepository;
        private readonly IVendedoresRepository _vendedoresRepository;

        private readonly decimal _factorCentecimal = 0.01m;

        public EncargosRepository(LocalConfig config) : base(config)
        { }

        public EncargosRepository(
            IClientesRepository clientesRepository,
            IProveedorRepository proveedorRepository, 
            IFarmacoRepository farmacoRepository, 
            ICategoriaRepository categoriaRepository, 
            IFamiliaRepository familiaRepository, 
            ILaboratorioRepository laboratorioRepository,
            IVendedoresRepository vendedoresRepository)
        {
            _clientesRepository = clientesRepository ?? throw new ArgumentNullException(nameof(clientesRepository));
            _proveedorRepository = proveedorRepository ?? throw new ArgumentNullException(nameof(proveedorRepository));
            _farmacoRepository = farmacoRepository ?? throw new ArgumentNullException(nameof(farmacoRepository));
            _categoriaRepository = categoriaRepository ?? throw new ArgumentNullException(nameof(categoriaRepository));
            _familiaRepository = familiaRepository ?? throw new ArgumentNullException(nameof(familiaRepository));
            _laboratorioRepository = laboratorioRepository ?? throw new ArgumentNullException(nameof(laboratorioRepository));
            _vendedoresRepository = vendedoresRepository ?? throw new ArgumentNullException(nameof(vendedoresRepository));
        }

        public IEnumerable<Encargo> GetAllByContadorGreaterOrEqual(int year, long? contador)
        {
            using (var db = FarmaciaContext.Create(_config))
            {
                var sql = @"SELECT TOP 1000 * From Encargo WHERE year(idFecha) >= @year AND IdContador >= @contador Order by IdContador ASC";
                return db.Database.SqlQuery<Encargo>(sql,
                    new SqlParameter("year", year),
                    new SqlParameter("contador", contador ?? SqlInt64.Null))
                    .ToList();
            }
        }

        public IEnumerable<Encargo> GetAllByIdGreaterOrEqual(int year, long encargo)
        {
            var rs = Enumerable.Empty<DTO.Encargo>();
            using (var db = FarmaciaContext.Proveedores())
            {
                var sql = @"SELECT ID_Encargo as Id, ID_Farmaco as Farmaco, ID_Cliente as Cliente, ID_Vendedor as Vendedor, Fecha_Hora as FechaHora, Fecha_Hora_Entrega as FechaHoraEntrega, Cantidad, Observaciones From Encargos WHERE year(Fecha_Hora) >= @year AND Id_Encargo >= @encargo Order by Id_Encargo ASC";
                rs = db.Database.SqlQuery<DTO.Encargo>(sql,
                    new OleDbParameter("year", year),
                    new OleDbParameter("encargos", (int) encargo))
                        .Take(10)
                        .ToList();
            }
            
            return rs.Select(GenerarEncargo);
        }

        private Encargo GenerarEncargo(DTO.Encargo encargo)
        {                        
            var cliente = _clientesRepository.GetOneOrDefaultById(encargo.Cliente ?? 0);
            var vendedor = _vendedoresRepository.GetOneOrDefaultById(encargo.Vendedor ?? 0);

            var farmacoEncargado = default(Farmaco);
            var farmaco = _farmacoRepository.GetOneOrDefaultById(encargo.Farmaco ?? 0);
            if (farmaco != null)
            {
                var pcoste = farmaco.PrecioUnicoEntrada.HasValue && farmaco.PrecioUnicoEntrada != 0
                    ? (decimal)farmaco.PrecioUnicoEntrada.Value * _factorCentecimal
                    : ((decimal?)farmaco.PrecioMedio ?? 0m) * _factorCentecimal;

                var proveedor = _proveedorRepository.GetOneOrDefaultByCodigoNacional(farmaco.Id);

                var categoria = farmaco.CategoriaId.HasValue
                    ? _categoriaRepository.GetOneOrDefaultById(farmaco.CategoriaId.Value)
                    : null;

                var subcategoria = farmaco.CategoriaId.HasValue && farmaco.SubcategoriaId.HasValue
                    ? _categoriaRepository.GetSubcategoriaOneOrDefaultByKey(
                        farmaco.CategoriaId.Value,
                        farmaco.SubcategoriaId.Value)
                    : null;

                var familia = _familiaRepository.GetOneOrDefaultById(farmaco.Familia);
                var laboratorio = _laboratorioRepository.GetOneOrDefaultByCodigo(farmaco.Laboratorio);

                farmacoEncargado = new Farmaco
                {
                    Id = farmaco.Id,
                    Codigo = encargo.Farmaco.ToString(),
                    PrecioCoste = pcoste,
                    Proveedor = proveedor,
                    Categoria = categoria,
                    Subcategoria = subcategoria,
                    Familia = familia,
                    Laboratorio = laboratorio,
                    Denominacion = farmaco.Denominacion,
                    Precio = farmaco.PVP * _factorCentecimal,
                    Stock = farmaco.Existencias ?? 0
                };
            }
            
            return new Encargo
            {
                Id = encargo.Id,
                Fecha = encargo.FechaHora ?? DateTime.MinValue,
                FechaEntrega = encargo.FechaHoraEntrega,
                Farmaco = farmacoEncargado,
                Cantidad = encargo.Cantidad,
                Cliente = cliente,
                Vendedor = vendedor,
                Observaciones = encargo.Observaciones
            };
        }

        public IEnumerable<Encargo> GetAllGreaterOrEqualByFecha(DateTime fecha)
        {
            using (var db = FarmaciaContext.Create(_config))
            {
                var sql = @"SELECT * From Encargo WHERE idFecha >= @fecha AND estado > 0 Order by idFecha DESC";
                return db.Database.SqlQuery<Encargo>(sql,
                    new SqlParameter("fecha", fecha))
                    .ToList();
            }
        }
    }
}