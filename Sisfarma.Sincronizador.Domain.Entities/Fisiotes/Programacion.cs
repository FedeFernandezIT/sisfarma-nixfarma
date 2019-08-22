namespace Sisfarma.Sincronizador.Domain.Entities.Fisiotes
{
    public class Programacion
    {
        public static readonly string Encendido = "encendido";
        public static readonly string Apagado = "apagado";

        public string horaT { get; set; }

        public string horaM { get; set; }
    }
}
