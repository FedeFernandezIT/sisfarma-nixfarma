using System;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class PuntoPendienteActualizacionSincronizador : TaskSincronizador
    {
        public PuntoPendienteActualizacionSincronizador(FarmaticService farmatic, FisiotesService fisiotes) 
            : base(farmatic, fisiotes)
        {
        }

        public override void Process() => ProcessUpdatePuntosPendientes(_farmatic, _fisiotes);

        private void ProcessUpdatePuntosPendientes(FarmaticService farmatic, FisiotesService fisiotes)
        {
            var puntos = fisiotes.PuntosPendientes.GetWithoutRedencion();
            foreach (var pto in puntos)
            {
                _cancellationToken.ThrowIfCancellationRequested();

                var venta = farmatic.Ventas.GetOneOrDefaultById(pto.idventa);
                if (venta != null)
                {
                    var lineas = farmatic.Ventas.GetLineasVentaByVenta(venta.IdVenta);
                    foreach (var linea in lineas)
                    {
                        var lineaRedencion =
                            farmatic.Ventas.GetOneOrDefaultLineaRedencionByKey(linea.IdVenta, linea.IdNLinea);

                        var redencion = lineaRedencion?.Redencion ?? 0;

                        var articulo = farmatic.Articulos.GetOneOrDefaultById(linea.Codigo);

                        var proveedor = (articulo != null)
                            ? farmatic.Proveedores.GetOneOrDefaultByCodigoNacional(articulo.IdArticu)?.FIS_NOMBRE ?? string.Empty
                            : string.Empty;                        

                        fisiotes.PuntosPendientes.Update(venta.TipoVenta, proveedor,
                            Convert.ToSingle(linea.DescuentoLinea), Convert.ToSingle(venta.DescuentoOpera),
                            Convert.ToSingle(redencion), linea.IdVenta, linea.IdNLinea);
                    }
                }
                else
                    fisiotes.PuntosPendientes.Update(pto.idventa);
            }
        }
    }
}
