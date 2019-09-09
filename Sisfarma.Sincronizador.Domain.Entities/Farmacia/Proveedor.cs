using System;

namespace Sisfarma.Sincronizador.Domain.Entities.Farmacia
{
    public class Proveedor
    {
        public long Id { get; set; }

        public string Codigo { get; set; }

        public string Nombre { get; set; }
    }

    public class ProveedorHistorico
    {
        public long Id { get; set; }

        public string FarmacoId { get; set; }

        public DateTime Fecha { get; set; }

        public decimal PUC { get; set; }
    }
}