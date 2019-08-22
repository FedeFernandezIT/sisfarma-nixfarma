using System;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IEntregasExternalService
    {
        bool Exists(int venta, int linea);
        void Insert(EntregaCliente ec);
        void Insert(int venta, int linea, string codigo, string descripcion, int cantidad, decimal numero, string tipoLinea, int fecha, string dni, string puesto, string trabajador, DateTime fechaVenta, float? pvp);
    }
}