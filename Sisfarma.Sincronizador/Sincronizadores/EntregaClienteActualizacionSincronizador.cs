using System;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Farmatic.Models;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using static Sisfarma.Sincronizador.Fisiotes.Repositories.ConfiguracionesRepository;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class EntregaClienteActualizacionSincronizador : TaskSincronizador
    {
        const string FIELD_POR_DONDE_VOY_ENTREGAS_CLIENTES = FieldsConfiguracion.FIELD_POR_DONDE_VOY_ENTREGAS_CLIENTES;
        private const string TRABAJADOR_DEFAULT = "NO";

        public EntregaClienteActualizacionSincronizador(FarmaticService farmatic, FisiotesService fisiotes) : 
            base(farmatic, fisiotes)
        {
        }

        public override void Process() => ProcessUpdateEntregasClientes(_farmatic, _fisiotes);

        private void ProcessUpdateEntregasClientes(FarmaticService farmatic, FisiotesService fisiotes)
        {            
            var valor = fisiotes.Configuraciones.GetByCampo(FIELD_POR_DONDE_VOY_ENTREGAS_CLIENTES);
            var venta = Convert.ToInt32(!string.IsNullOrEmpty(valor)
                ? valor
                : "0");

            var ventasVirtuales = farmatic.Ventas.GetVirtualesLessThanId(venta);            
            foreach (var @virtual in ventasVirtuales)
            {
                _cancellationToken.ThrowIfCancellationRequested();

                var lineas = farmatic.Ventas.GetLineasVirtualesByVenta(@virtual.IdVenta);
                foreach (var linea in lineas)
                {
                    if (!fisiotes.Entregas.Exists(linea.IdVenta, linea.IdNLinea))
                    {            
                        fisiotes.Entregas.Insert(
                            GenerarEntregaCliente(farmatic, @virtual, linea));                        

                        fisiotes.Configuraciones.Update(FIELD_POR_DONDE_VOY_ENTREGAS_CLIENTES, @virtual.IdVenta.ToString());
                    }
                }
            }
        }

        private EntregaCliente GenerarEntregaCliente(FarmaticService farmatic, Venta @virtual, LineaVentaVirtual linea)
        {            
            var trabajador = farmatic.Vendedores
                .GetOneOrDefaultById(Convert.ToInt16(@virtual.XVend_IdVendedor))?.NOMBRE
                    ?? TRABAJADOR_DEFAULT;            

            return new EntregaCliente
            {
                idventa = linea.IdVenta,
                idnlinea = linea.IdNLinea,
                codigo = linea.Codigo.Strip(),
                descripcion = linea.Descripcion,
                cantidad = linea.Cantidad,
                precio = Convert.ToDecimal(linea.ImporteNeto),
                tipo = linea.TipoLinea,
                fecha = Convert.ToInt32(@virtual.FechaHora.ToString("yyyyMMdd")),
                dni = @virtual.XClie_IdCliente.Strip(),
                puesto = @virtual.Maquina,
                trabajador = trabajador,
                pvp = Convert.ToSingle(linea.Pvp),
                fechaEntrega = @virtual.FechaHora
            };            
        }
    }
}
