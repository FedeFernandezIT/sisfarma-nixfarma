namespace Sisfarma.Sincronizador.Farmatic.Models
{
    public partial class Destinatario
    {
        public Destinatario()
        {
        }
    
        public int Codigo { get; set; }

        public string fk_Cliente_1 { get; set; }

        public string fk_Paciente_1 { get; set; }

        public int fk_TipoDestinat_1 { get; set; }

        public short? fk_Vendedor_1 { get; set; }

        public string Nombre { get; set; }

        public string TlfMovil { get; set; }

        public string Email { get; set; }

        public bool AlertaSMS { get; set; }

        public bool AlertaEmail { get; set; }

        public short? Opciones { get; set; }       

    }
}
