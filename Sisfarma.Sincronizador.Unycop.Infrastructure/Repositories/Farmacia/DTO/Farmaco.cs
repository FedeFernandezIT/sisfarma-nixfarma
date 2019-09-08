using System;

namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia.DTO
{
    public class Farmaco
    {
        public int Id { get; set; }

        public string Codigo { get; set; }

        public decimal PrecioCoste { get; set; }

        public decimal Precio { get; set; }

        public decimal? PrecioMedio { get; set; }

        public decimal? PrecioUnicoEntrada { get; set; }

        public int Familia { get; set; }

        public string SubFamilia { get; set; }

        public int? CategoriaId { get; set; }

        public int? SubcategoriaId { get; set; }

        public long? Laboratorio { get; set; }

        public string Denominacion { get; set; }

        public DateTime? FechaUltimaCompra { get; set; }

        public DateTime? FechaUltimaVenta { get; set; }

        public string Ubicacion { get; set; }

        public bool BolsaPlastico { get; set; }

        public byte IVA { get; set; }

        public int PVP { get; set; }

        public long Stock { get; set; }

        public short? Existencias { get; set; }

        public DateTime? FechaBaja { get; set; }

        public DateTime? FechaCaducidad { get; set; }

        public string CodigoBarras { get; set; }

        public string CodigoImpuesto { get; set; }

        public decimal PUC { get; set; }

        public string Clase { get; set; }

        public string ClaseBot { get; set; }

        public long StockMinimo { get; set; }

        public long StockMaximo { get; set; }
    }
}