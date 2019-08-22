using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes.DTO;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class PuntoPendienteActualizacionSincronizador : DC.PuntoPendienteActualizacionSincronizador
    {
        public PuntoPendienteActualizacionSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        { }

        public override void Process()
        {
            var puntos = _sisfarma.PuntosPendientes.GetWithoutRedencion();
            foreach (var pto in puntos)
            {
                _cancellationToken.ThrowIfCancellationRequested();

                var venta = _farmacia.Ventas.GetOneOrDefaultById(pto.VentaId);
                if (venta != null)
                {
                    foreach (var item in venta.Detalle)
                    {
                        _sisfarma.PuntosPendientes.Sincronizar(new UpdatePuntacion
                        {
                            tipoPago = venta.Tipo,
                            proveedor = item.Farmaco.Proveedor?.Nombre ?? string.Empty,
                            idventa = pto.VentaId,
                            cod_nacional = item.Farmaco.Codigo
                        });
                    }
                }
                else _sisfarma.PuntosPendientes.Sincronizar(new UpdatePuntacion
                {
                    tipoPago = venta.Tipo,
                    idventa = pto.VentaId
                });
            }
        }        
    }
}
