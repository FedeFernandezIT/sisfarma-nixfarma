namespace Sisfarma.Sincronizador.Farmatic.Models
{
    public partial class Familia
    {
        public Familia()
        {     
        }
        
        public short IdFamilia { get; set; }

        public string Descripcion { get; set; }

        public double? Coef1 { get; set; }

        public double? Coef2 { get; set; }

        public bool DeducibleHacienda { get; set; }

        public double? FactorDtoCredito { get; set; }

        public string XGrup_IdGrupoIva { get; set; }

        public int? desviacion { get; set; }

        public short? GRUPOFORMU { get; set; }

        public int? Empresa { get; set; }

        public int? InfoEspecial { get; set; }
        
    }
}
