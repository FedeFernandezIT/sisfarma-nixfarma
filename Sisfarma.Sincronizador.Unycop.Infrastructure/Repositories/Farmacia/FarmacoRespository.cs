using Sisfarma.Sincronizador.Core.Config;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Unycop.Infrastructure.Data;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Linq;

using DC = Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia
{
    public interface IFarmacoRepository
    {
        DTO.Farmaco GetOneOrDefaultById(long id);
    }

    public class FarmacoRespository : FarmaciaRepository, IFarmacoRepository, DC.IFarmacosRepository
    {
        private readonly ICategoriaRepository _categoriaRepository;
        private readonly ICodigoBarraRepository _barraRepository;        
        private readonly DC.IFamiliaRepository _familiaRepository;
        private readonly DC.ILaboratorioRepository _laboratorioRepository;
        private readonly DC.IProveedorRepository _proveedorRepository;

        private readonly decimal _factorCentecimal = 0.01m;

        public FarmacoRespository(LocalConfig config) 
            : base(config)
        { }

        public FarmacoRespository() { }

        public FarmacoRespository(
            ICategoriaRepository categoriaRepository, 
            ICodigoBarraRepository barraRepository, 
            DC.IFamiliaRepository familiaRepository, 
            DC.ILaboratorioRepository laboratorioRepository, 
            DC.IProveedorRepository proveedorRepository)
        {
            _categoriaRepository = categoriaRepository ?? throw new ArgumentNullException(nameof(categoriaRepository));
            _barraRepository = barraRepository ?? throw new ArgumentNullException(nameof(barraRepository));
            _familiaRepository = familiaRepository ?? throw new ArgumentNullException(nameof(familiaRepository));
            _laboratorioRepository = laboratorioRepository ?? throw new ArgumentNullException(nameof(laboratorioRepository));
            _proveedorRepository = proveedorRepository ?? throw new ArgumentNullException(nameof(proveedorRepository));
        }      

        public DTO.Farmaco GetOneOrDefaultById(long id)
        {
            var idInteger = (int)id;
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"SELECT ID_Farmaco as Id, Denominacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, PVP, Existencias, Familia, CategoriaId, SubcategoriaId, Laboratorio FROM Farmacos WHERE ID_Farmaco= @id";
                return db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("id", id))
                    .FirstOrDefault();
            }            
        }

        public IEnumerable<Farmaco> GetAllByFechaUltimaEntradaGreaterOrEqual(DateTime fecha)
        {
            var rs = Enumerable.Empty<DTO.Farmaco>();
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE Fecha_U_Entrada >= @fecha ORDER BY Fecha_U_Entrada ASC";
                rs =  db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("fecha", fecha.ToDateInteger("yyyyMMdd")))                    
                    .ToList();
            }

            return rs.Select(GenerarFarmaco);
        }

        public IEnumerable<DTO.Farmaco> GetAllByFechaUltimaEntradaGreaterOrEqualAsDTO(DateTime fecha)
        {            
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE Fecha_U_Entrada >= @fecha ORDER BY Fecha_U_Entrada ASC";
                return db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("fecha", fecha.ToDateInteger("yyyyMMdd")))             
                    .ToList();
            }            
        }

        public IEnumerable<Farmaco> GetAllByFechaUltimaSalidaGreaterOrEqual(DateTime fecha)
        {
            var rs = Enumerable.Empty<DTO.Farmaco>();
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE Fecha_U_Salida >= @fecha ORDER BY Fecha_U_Salida ASC";
                rs = db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("fecha", fecha.ToDateInteger("yyyyMMdd")))        
                    .ToList();
            }

            return rs.Select(GenerarFarmaco);
        }

        public IEnumerable<DTO.Farmaco> GetAllByFechaUltimaSalidaGreaterOrEqualAsDTO(DateTime fecha)
        {            
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE Fecha_U_Salida >= @fecha ORDER BY Fecha_U_Salida ASC";
                return db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("fecha", fecha.ToDateInteger("yyyyMMdd")))
                    .ToList();
            }          
        }

        public IEnumerable<Farmaco> GetAllWithoutStockByIdGreaterOrEqual(string codigo)
        {
            var rs = Enumerable.Empty<DTO.Farmaco>();
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE ID_Farmaco >= @codigo AND (existencias <= 0 OR existencias IS NULL) ORDER BY ID_Farmaco ASC";
                rs = db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("codigo", int.Parse(codigo)))                    
                    .ToList();
            }

            return rs.Select(GenerarFarmaco);
        }

        public IEnumerable<DTO.Farmaco> GetAllWithoutStockByIdGreaterOrEqualAsDTO(string codigo)
        {            
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE ID_Farmaco >= @codigo AND (existencias <= 0 OR existencias IS NULL) ORDER BY ID_Farmaco ASC";
                return db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("codigo", int.Parse(codigo)))                    
                    .ToList();
            }            
        }

        public IEnumerable<Farmaco> GetWithStockByIdGreaterOrEqual(string codigo)
        {
            var rs = Enumerable.Empty<DTO.Farmaco>();
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE ID_Farmaco >= @codigo AND existencias > 0 ORDER BY ID_Farmaco ASC";
                rs = db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("codigo", int.Parse(codigo)))                    
                    .ToList();
            }

            return rs.Select(GenerarFarmaco);
        }

        public IEnumerable<DTO.Farmaco> GetWithStockByIdGreaterOrEqualAsDTO(string codigo)
        {            
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 999 ID_Farmaco as Id, Familia, CategoriaId, SubcategoriaId, Fecha_U_Entrada as FechaUltimaEntrada, Fecha_U_Salida as FechaUltimaSalida, Ubicacion, PC_U_Entrada as PrecioUnicoEntrada, PCMedio as PrecioMedio, BolsaPlastico, PVP, IVA, Stock, Existencias, Denominacion, Laboratorio, FechaBaja, Fecha_Caducidad as FechaCaducidad from Farmacos WHERE ID_Farmaco >= @codigo AND existencias > 0 ORDER BY ID_Farmaco ASC";
                return db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("codigo", int.Parse(codigo)))
                    .ToList();
            }            
        }

        public bool AnyGraterThatDoesnHaveStock(string codigo)
        {
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 1 ID_Farmaco as Id FROM Farmacos WHERE ID_Farmaco > @codigo AND existencias <= 0 ORDER BY ID_Farmaco ASC";
                var rs = db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("codigo", int.Parse(codigo)))
                    .FirstOrDefault();

                return rs != null;
            }
        }

        public bool AnyGreaterThatHasStock(string codigo)
        {
            using (var db = FarmaciaContext.Farmacos())
            {
                var sql = @"select top 1 ID_Farmaco as Id FROM Farmacos WHERE ID_Farmaco > @codigo AND existencias > 0 ORDER BY ID_Farmaco ASC";
                var rs = db.Database.SqlQuery<DTO.Farmaco>(sql,
                    new OleDbParameter("codigo", int.Parse(codigo)))
                    .FirstOrDefault();

                return rs != null;
            }
        }

        public Farmaco GenerarFarmaco(DTO.Farmaco farmaco)
        {
            var familia = _familiaRepository.GetOneOrDefaultById(farmaco.Familia);
            var categoria = farmaco.CategoriaId.HasValue
                            ? _categoriaRepository.GetOneOrDefaultById(farmaco.CategoriaId.Value)
                            : null;

            var subcategoria = farmaco.CategoriaId.HasValue && farmaco.SubcategoriaId.HasValue
                ? _categoriaRepository.GetSubcategoriaOneOrDefaultByKey(
                    farmaco.CategoriaId.Value,
                    farmaco.SubcategoriaId.Value)
                : null;

            var codigoBarra = _barraRepository.GetOneByFarmacoId(farmaco.Id);

            var proveedor = _proveedorRepository.GetOneOrDefaultByCodigoNacional(farmaco.Id);

            var laboratorio = _laboratorioRepository.GetOneOrDefaultByCodigo(farmaco.Laboratorio);

            var pcoste = farmaco.PrecioUnicoEntrada.HasValue && farmaco.PrecioUnicoEntrada != 0
                            ? (decimal)farmaco.PrecioUnicoEntrada.Value * _factorCentecimal
                            : ((decimal?)farmaco.PrecioMedio ?? 0m) * _factorCentecimal;

            var iva = default(decimal);
            switch (farmaco.IVA)
            {
                case 1: iva = 4; break;

                case 2: iva = 10; break;

                case 3: iva = 21; break;

                default: iva = 0; break;
            }

            return new Farmaco
            {
                Id = farmaco.Id,
                Codigo = farmaco.Id.ToString(),
                Denominacion = farmaco.Denominacion,
                Familia = familia,
                Categoria = categoria,
                Subcategoria = subcategoria,
                CodigoBarras = codigoBarra,
                Proveedor = proveedor,
                FechaUltimaCompra = farmaco.FechaUltimaEntrada.HasValue ? (DateTime?)$"{farmaco.FechaUltimaEntrada.Value}".ToDateTimeOrDefault("yyyyMMdd") : null,
                FechaUltimaVenta = farmaco.FechaUltimaSalida.HasValue ? (DateTime?)$"{farmaco.FechaUltimaSalida.Value}".ToDateTimeOrDefault("yyyyMMdd") : null,
                Ubicacion = farmaco.Ubicacion ?? string.Empty,
                Web = farmaco.BolsaPlastico,
                Precio = farmaco.PVP * _factorCentecimal,
                PrecioCoste = pcoste,
                Iva = iva,
                Stock = farmaco.Existencias ?? 0,
                StockMinimo = farmaco.Stock ?? 0,
                Laboratorio = laboratorio,
                Baja = farmaco.FechaBaja > 0,
                FechaCaducidad = farmaco.FechaCaducidad.HasValue ? (DateTime?)$"{farmaco.FechaCaducidad.Value}".ToDateTimeOrDefault("yyyyMM") : null
            };
        }

        public bool Exists(string codigo) => GetOneOrDefaultById(codigo.ToIntegerOrDefault()) != null;
    }
}
