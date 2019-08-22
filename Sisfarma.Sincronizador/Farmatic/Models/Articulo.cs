namespace Sisfarma.Sincronizador.Farmatic.Models
{
    using System;

    public partial class Articulo
    {
        public Articulo()
        {    
        }
        
        public string IdArticu { get; set; }

        public string Descripcion { get; set; }

        public string Presentacion { get; set; }

        public string Situacion { get; set; }

        public short XFam_IdFamilia { get; set; }

        public string Laboratorio { get; set; }

        public double Pvp { get; set; }

        public double PvpAux { get; set; }

        public double Puc { get; set; }

        public double Pmc { get; set; }

        public int StockActual { get; set; }

        public int StockMinimo { get; set; }

        public int StockMaximo { get; set; }

        public int LoteOptimo { get; set; }

        public DateTime? FechaUltimaEntrada { get; set; }

        public DateTime? FechaUltimaSalida { get; set; }

        public DateTime? FechaCaducidad { get; set; }

        public string ProveedorHabitual { get; set; }

        public short? CarteraHabitual { get; set; }

        public string XGrup_IdGrupoIva { get; set; }

        public bool Efp { get; set; }

        public bool Receta { get; set; }

        public bool Psicotropo { get; set; }

        public bool Estupefaciente { get; set; }

        public bool Frigorifico { get; set; }

        public bool Cicero { get; set; }

        public bool Caducidad { get; set; }

        public bool Visado { get; set; }

        public bool Ecm { get; set; }

        public bool ExcluidoSS { get; set; }

        public bool Baja { get; set; }

        public bool UsoH { get; set; }

        public bool DiagnosticoH { get; set; }

        public bool Tld { get; set; }

        public string GruTera { get; set; }

        public string ActualizaStock { get; set; }

        public double? AcumDias { get; set; }

        public double? AcumStock { get; set; }

        public double? DtoFijo { get; set; }

        public int? desviacion { get; set; }

        public int? MarcaEx { get; set; }
    }
}
