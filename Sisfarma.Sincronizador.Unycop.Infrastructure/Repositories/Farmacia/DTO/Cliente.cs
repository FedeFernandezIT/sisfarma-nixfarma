namespace Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia.DTO
{
    public class Cliente
    {     
        public int Id { get; set; }

        public string Tarjeta { get; set; }

        public string EstadoCivil { get; set; }

        public string Movil { get; set; }

        public string Telefono { get; set; }

        public string Correo { get; set; }

        public int FechaNacimiento { get; set; }

        public double Puntos { get; set; }

        public string DNICIF { get; set; }

        public bool LOPD { get; set; }

        public string Sexo { get; set; }

        public int Baja { get; set; }

        public int FechaAlta { get; set; }

        public string Direccion { get; set; }

        public string Localidad { get; set; }

        public string CodigoPostal { get; set; }

        public string Nombre { get; set; }
    }
}
