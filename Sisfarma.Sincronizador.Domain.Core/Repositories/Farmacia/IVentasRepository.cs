using System;
using System.Collections.Generic;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;

namespace Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia
{
    public interface IVentasRepository
    {
        List<Venta> GetAllByIdGreaterOrEqual(int year, long value);

        List<Venta> GetAllByIdGreaterOrEqual(int year, DateTime timestamp);

        List<Venta> GetAllByIdGreaterOrEqual(long venta, DateTime fecha);

        List<VentaDetalle> GetDetalleDeVentaByVentaId(long venta);

        Ticket GetOneOrDefaultTicketByVentaId(long venta);

        //List<LineaVentaVirtual> GetLineasVirtualesByVenta(int venta);
        //VentaDetalle GetLineaVentaOrDefaultByKey(long venta, long linea);
        Venta GetOneOrDefaultById(long venta);

        //LineaVentaRedencion GetOneOrDefaultLineaRedencionByKey(int venta, int linea);
        //List<Venta> GetVirtualesLessThanId(long venta);
    }
}