using System;

namespace Sisfarma.Sincronizador.Domain.Entities
{
    public class ClienteDto
    {
        public string Movil { get; set; }
        public string Email { get; set; }
        public string Trabajador { get; set; }
        public string Tarjeta { get; set; }
        public string Nombre { get; set; }
        public string Telefono { get; set; }
        public string Direccion { get; set; }
        public decimal Puntos { get; set; }
        public long FechaNacimiento { get; set; }
        public string Sexo { get; set; }
        public DateTime? FechaAlta { get; set; }
        public int Baja { get; set; }
        public int Lopd { get; set; }
    }
}
