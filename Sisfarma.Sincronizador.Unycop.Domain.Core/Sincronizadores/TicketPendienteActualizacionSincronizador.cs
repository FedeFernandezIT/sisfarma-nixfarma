using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes.DTO;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class TicketPendienteActualizacionSincronizador : TaskSincronizador
    {
        public TicketPendienteActualizacionSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        { }

        public override void Process()
        {
            var puntos = _sisfarma.PuntosPendientes.GetWithoutTicket();
            foreach (var pto in puntos)
            {
                _cancellationToken.ThrowIfCancellationRequested();

                var ticket = _farmacia.Ventas.GetOneOrDefaultTicketByVentaId(pto.VentaId);
                if (ticket != null)
                {                    
                    _sisfarma.PuntosPendientes.Sincronizar(new UpdateTicket
                    {
                        numTicket = ticket.Numero,
                        serie = ticket.Serie,
                        idventa = pto.VentaId
                    });                    
                }
                else _sisfarma.PuntosPendientes.Sincronizar(new UpdateTicket
                {
                    numTicket = 0,
                    serie = string.Empty,
                    idventa = pto.VentaId
                });
            }
        }
    }
}
