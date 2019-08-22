namespace Sisfarma.Sincronizador.Farmatic.Models
{
    using System;

    public partial class Venta
    {
        public int IdVenta { get; set; }

        public DateTime FechaHora { get; set; }

        public short Ejercicio { get; set; }

        public short Mes { get; set; }

        public string XClie_IdCliente { get; set; }

        public short XVend_IdVendedor { get; set; }

        public string Usuario { get; set; }

        public string Maquina { get; set; }

        public string TipoVenta { get; set; }

        public double TotalBase { get; set; }

        public double TotalCuota { get; set; }

        public double TotalRecargo { get; set; }

        public double TotalVentaBruta { get; set; }

        public double? DescuentoLinea { get; set; }

        public double? DescuentoOpera { get; set; }

        public double TotalVenta { get; set; }

        public string RecetaPendiente { get; set; }

        public string Facturada { get; set; }

        public string NumeroDoc { get; set; }

        public int? IdContador { get; set; }

        public int? Empresa { get; set; }

    }
}
