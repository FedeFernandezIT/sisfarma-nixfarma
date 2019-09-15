using System;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes.DTO;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class VentaPendienteSincronizador : TaskSincronizador
    {
        private int _anioInicio;
        private string _verCategorias;
        private string _puntosDeSisfarma;
        private string _cargarPuntos;

        public VentaPendienteSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes)
            : base(farmacia, fisiotes)
        { }

        public override void LoadConfiguration()
        {
            base.LoadConfiguration();
            _anioInicio = ConfiguracionPredefinida[Configuracion.FIELD_ANIO_INICIO]
                .ToIntegerOrDefault(@default: DateTime.Now.Year - 2);
            _verCategorias = ConfiguracionPredefinida[Configuracion.FIELD_VER_CATEGORIAS];
            _puntosDeSisfarma = ConfiguracionPredefinida[Configuracion.FIELD_PUNTOS_SISFARMA];
            _cargarPuntos = ConfiguracionPredefinida[Configuracion.FIELD_CARGAR_PUNTOS] ?? "no";
        }

        public override void PreSincronizacion()
        {
            base.PreSincronizacion();
        }

        public override void Process()
        {
            var puntos = _sisfarma.PuntosPendientes.GetWithoutRedencion();
            foreach (var pto in puntos)
            {
                _cancellationToken.ThrowIfCancellationRequested();

                var ventaSerial = pto.VentaId.ToString();
                var numeroVenta = long.Parse(ventaSerial.SubstringEnd(5));
                var empresa = ventaSerial.Substring(ventaSerial.Length - 5) == "00001" ? "EMP1" : "EMP2";
                var venta = _farmacia.Ventas.GetOneOrDefaultById(numeroVenta, empresa, _anioInicio);
                if (venta != null)
                {
                    var detalle = _farmacia.Ventas.GetDetalleDeVentaPendienteByVentaId(numeroVenta, empresa);
                    foreach (var item in detalle)
                    {
                        if (item.Situacion != "A")
                        {
                            _sisfarma.PuntosPendientes.Sincronizar(new UpdatePuntuacion
                            {
                                tipoPago = venta.TipoOperacion,
                                proveedor = item.Farmaco.Proveedor?.Nombre ?? string.Empty,
                                idventa = pto.VentaId,
                                idnlinea = item.Linea,
                            });
                        }
                        _sisfarma.PuntosPendientes.Sincronizar(new DeletePuntuacion { idventa = pto.VentaId, idnlinea = item.Linea });
                    }
                }
                else _sisfarma.PuntosPendientes.Sincronizar(new DeletePuntuacion { idventa = pto.VentaId });
            }
        }
    }
}