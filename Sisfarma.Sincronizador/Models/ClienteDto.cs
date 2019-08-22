using System;

namespace Sisfarma.Sincronizador.Models
{
    public class ClienteDto
    {
        public string Movil { get; internal set; }
        public string Email { get; internal set; }
        public string Trabajador { get; internal set; }
        public string Tarjeta { get; internal set; }
        public string Nombre { get; internal set; }
        public string Telefono { get; internal set; }
        public string Direccion { get; internal set; }
        public decimal Puntos { get; internal set; }
        public long FechaNacimiento { get; internal set; }
        public string Sexo { get; internal set; }
        public DateTime? FechaAlta { get; internal set; }
        public int Baja { get; internal set; }
        public int Lopd { get; internal set; }
    }
}
