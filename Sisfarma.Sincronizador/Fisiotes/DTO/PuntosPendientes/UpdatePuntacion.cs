namespace Sisfarma.Sincronizador.Fisiotes.DTO.PuntosPendientes
{
    public class UpdatePuntacion
    {        
        public long idventa { get; set; }

        public long idnlinea { get; set; }
        
        public int cantidad { get; set; }

        public decimal precio { get; set; }

        public string tipoPago { get; set; }

        public string dni { get; set; }        

        public string trabajador { get; set; }        

        public string receta { get; set; }
        
        public float? dtoLinea { get; set; }

        public float? dtoVenta { get; set; }        
    }
}
