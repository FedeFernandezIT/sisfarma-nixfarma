using System;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.Repositories.Farmacia.DTO
{
    public class Farmaco
    {
        public int Id { get; set; }

        public float? PrecioMedio { get; set; }

        public float? PrecioUnicoEntrada { get; set; }

        public byte Familia { get; set; }

        public int? CategoriaId { get; set; }

        public int? SubcategoriaId { get; set; }

        public string Laboratorio { get; set; }

        public string Denominacion { get; set; }

        public int? FechaUltimaEntrada { get; set; }

        public int? FechaUltimaSalida { get; set; }

        public string Ubicacion { get; set; }

        public bool BolsaPlastico { get; set; }

        public byte IVA { get; set; }

        public int PVP { get; set; }

        public short? Stock { get; set; }

        public short? Existencias { get; set; }

        public int FechaBaja { get; set; }

        /// <summary>
        /// Fecha formato yyyyMM
        /// </summary>
        public int? FechaCaducidad { get; set; }
    }
}
