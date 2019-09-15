using System;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Domain.Entities.Farmacia
{
    public partial class Recepcion
    {
        public long Id { get; set; }

        public long Pedido { get; set; }

        public DateTime Fecha { get; set; }

        public DateTime Hora { get; set; }

        public string XProv_IdProveedor { get; set; }

        public short? XCPro_IdCondicion { get; set; }

        public short XVend_IdVendedor { get; set; }

        public int Actualizado { get; set; }

        public short? Ejercicio { get; set; }

        public short? Mes { get; set; }

        public Proveedor Proveedor { get; set; }

        public ICollection<RecepcionDetalle> Detalle { get; set; }

        public Recepcion()
            => Detalle = new HashSet<RecepcionDetalle>();

        public bool HasDetalle() => Detalle != null && Detalle.Count > 0;

        public bool HasProveedor() => this.Proveedor != null;

        public int Lineas { get; set; }

        public decimal ImportePVP { get; set; }

        public decimal ImportePUC { get; set; }

        public Recepcion AddRangeDetalle(List<RecepcionDetalle> detalle)
        {
            foreach (var item in detalle)
            {
                item.Recepcion = this;
                Detalle.Add(item);
            }
            return this;
        }
    }

    public class RecepcionDetalle
    {
        public int Id { get; set; }

        public long RecepcionId { get; set; }

        public long Linea { get; set; }

        public Recepcion Recepcion { get; set; }

        public Farmaco Farmaco { get; set; }

        public decimal PVP { get; set; }

        public decimal PUC { get; set; }

        public long Cantidad { get; set; }

        public int CantidadBonificada { get; set; }
    }
}