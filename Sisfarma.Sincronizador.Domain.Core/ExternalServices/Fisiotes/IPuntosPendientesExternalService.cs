using System;
using System.Collections.Generic;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes.DTO;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IPuntosPendientesExternalService : IPuntosPendientesExternalServiceNew
    {
        bool Exists(int venta, int linea);

        bool ExistsGreatThanOrEqual(DateTime fecha);

        long GetLastOfYear(int year);

        IEnumerable<PuntosPendientes> GetOfRecetasPendientes(int año);

        PuntosPendientes GetOneOrDefaultByItemVenta(int venta, int linea);

        decimal GetPuntosByDni(int dni);

        decimal GetPuntosCanjeadosByDni(int dni);

        DateTime GetTimestampUltimaVentaByEmpresa(string empresa);

        IEnumerable<PuntosPendientes> GetWithoutRedencion();

        IEnumerable<PuntosPendientes> GetWithoutTicket();

        void Insert(IEnumerable<PuntosPendientes> pps);

        void Insert(int venta, int linea, string codigoBarra, string codigo, string descripcion, string familia, int cantidad, decimal numero, string tipoPago, int fecha, string dni, string cargado, string puesto, string trabajador, string codLaboratorio, string laboratorio, string proveedor, string receta, DateTime fechaVenta, string superFamlia, float precioMed, float pcoste, float dtoLinea, float dtoVta, float redencion, string recetaPendiente);

        bool AnyWithoutPagoGreaterThanVentaId(long ultimaVenta);

        void Insert(PuntosPendientes pp);

        void InsertPuntuacion(InsertPuntuacion pp);

        void Update(long venta);

        void Update(long venta, long linea, string receta = "C");

        void Update(string tipoPago, string proveedor, float? dtoLinea, float? dtoVenta, float redencion, long venta, long linea);

        void Sincronizar(UpdatePuntuacion pp);

        void Sincronizar(DeletePuntuacion pp);

        void Sincronizar(UpdateTicket tk);
    }

    public interface IPuntosPendientesExternalServiceNew
    {
        void Sincronizar(PuntosPendientes punto, bool calcularPuntos = false);

        void Sincronizar(IEnumerable<PuntosPendientes> puntos);
    }
}