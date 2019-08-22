using System;

namespace Sisfarma.Sincronizador.Farmatic.DTO.Recepciones
{
    public class RecepcionGroup
    {
        public string XArt_IdArticu { get; set; }

        public string XProv_IdProveedor { get; set; }

        public DateTime Hora { get; set; }        

        public double ImportePuc { get; set; }
    }
}
