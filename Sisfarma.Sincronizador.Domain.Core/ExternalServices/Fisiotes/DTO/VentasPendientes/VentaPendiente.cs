namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes.DTO.VentasPendientes
{
    public class VentaPendiente
    {
        public long idventa { get; set; }

        public string empresa { get; set; }
    }

    public class DeleteVentaPendiente
    {
        public long idventa { get; set; }

        public string empresa { get; set; }
    }
}