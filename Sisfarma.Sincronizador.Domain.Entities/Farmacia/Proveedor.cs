using System;

namespace Sisfarma.Sincronizador.Domain.Entities.Farmacia
{
    public class Proveedor
    {
        public int Id { get; set; }

        public string Codigo { get; set; }

        public string Nombre { get; set; }
    }

    public class ProveedorHistorico
    {
        public int Id { get; set; }

        public long FarmacoId { get; set; }

        public DateTime Fecha { get; set; }

        public decimal PUC { get; set; }
    }
}
