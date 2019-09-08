using System;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Domain.Entities.Farmacia
{
    public class Pedido
    {
        public long Id { get; set; }

        public Proveedor Proveedor { get; set; }

        public bool Modem { get; set; }

        public int? NLineas { get; set; }

        public double? ImportePvp { get; set; }

        public double? ImportePuc { get; set; }

        public DateTime? Fecha { get; set; }

        public DateTime? Hora { get; set; }

        public string Representante { get; set; }

        public DateTime? FecVisita { get; set; }

        public string TelfContacto { get; set; }

        public DateTime? FecProxVisita { get; set; }

        public int? Tipo { get; set; }

        public ICollection<PedidoDetalle> Detalle { get; set; }

        public long Numero { get; set; }

        public string Empresa { get; set; }

        public Pedido()
            => Detalle = new HashSet<PedidoDetalle>();

        public bool HasDetalle() => Detalle != null && Detalle.Count > 0;

        public bool HasProveedor() => this.Proveedor != null;

        public Pedido AddRangeDetalle(List<PedidoDetalle> detalle)
        {
            foreach (var item in detalle)
            {
                item.Pedido = this;
                Detalle.Add(item);
            }
            return this;
        }
    }

    public partial class PedidoDetalle
    {
        public int Id { get; set; }

        public long PedidoId { get; set; }

        public int Linea { get; set; }

        public long CantidadPedida { get; set; }

        public bool GestionFaltas { get; set; }

        public decimal PVP { get; set; }

        public decimal PUC { get; set; }

        public string FarmacoCodigo { get; set; }

        public Farmaco Farmaco { get; set; }

        public long PedidoCodigo { get; set; }

        public Pedido Pedido { get; set; }

        public string EmpresaCodigo { get; set; }

        public bool HasFarmaco() => Farmaco != null;
    }
}