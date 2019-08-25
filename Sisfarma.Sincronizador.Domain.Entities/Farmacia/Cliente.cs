using System;

namespace Sisfarma.Sincronizador.Domain.Entities.Farmacia
{
    public class Cliente
    {
        public Cliente()
        { }

        public long Id { get; set; }

        public string Tarjeta { get; set; }

        public string EstadoCivil { get; set; }

        public string Celular { get; set; }

        public string Telefono { get; set; }

        public string Email { get; set; }

        public DateTime? FechaNacimiento { get; set; }

        public decimal Puntos { get; set; }

        public string NumeroIdentificacion { get; set; }

        public bool LOPD { get; set; }

        public string Sexo { get; set; }

        public bool Baja { get; set; }

        public DateTime? FechaAlta { get; set; }

        public DateTime? FechaBaja { get; set; }

        public string Direccion { get; set; }

        public string Localidad { get; set; }

        public string CodigoPostal { get; set; }

        public string NombreCompleto { get; set; }

        public string Apellido { get; set; }

        public string Nombre { get; set; }

        public string Trabajador { get; set; }

        public long CodigoCliente { get; set; }

        public long CodigoDes { get; set; }

        public bool HasTarjeta() => !string.IsNullOrWhiteSpace(Tarjeta);
    }
}