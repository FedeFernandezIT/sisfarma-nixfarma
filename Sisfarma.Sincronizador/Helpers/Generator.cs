using Sisfarma.Sincronizador.Consejo;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Farmatic.Models;
using Sisfarma.Sincronizador.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Sisfarma.Sincronizador.Helpers
{
    public static class Generator
    {
        private static readonly string COD_BARRAS_DEFAULT = "847000";
        private static readonly string LABORATORIO_DEAFULT = "<Sin Laboratorio>";

        public static ClienteDto GenerarCliente(FarmaticService farmaticService, Cliente cliente, bool hasSexo)
        {
            var localDestinatarios = farmaticService.Destinatarios.GetByCliente(cliente.IDCLIENTE);
            var movil = string.Empty;
            var email = string.Empty;
            if (localDestinatarios.Any())
            {
                movil = localDestinatarios.First().TlfMovil?.Trim() ?? movil;
                email = localDestinatarios.First().Email?.Trim() ?? email;
            }

            var fechaNacimiento = default(long);
            var sexo = string.Empty;

            var localAuxiliar = farmaticService.Clientes.GetAuxiliarById<ClienteAuxWithSexo>(cliente.IDCLIENTE);

            if (hasSexo)
            {                
                if (localAuxiliar != null)
                {
                    fechaNacimiento = Convert.ToInt64(localAuxiliar.FechaNac?.ToString("yyyyMMdd"));
                    sexo = localAuxiliar.Sexo == "V" ? "Hombre"
                        : localAuxiliar.Sexo == "M" ? "Mujer"
                        : string.Empty;
                }
            }
            else
            {                
                if (localAuxiliar != null)
                    fechaNacimiento = Convert.ToInt64(localAuxiliar.FechaNac?.ToString("yyyyMMdd"));
            }

            // Si sexo aún no tiene valor, le establecemos uno.
            if (sexo == string.Empty && cliente.FIS_NOMBRE != null)
                sexo = cliente.FIS_NOMBRE;

            var baja = string.IsNullOrEmpty(cliente.FIS_NIF) ||
                cliente.FIS_NIF.Trim().Equals("No") ||
                cliente.FIS_NIF.Trim().Equals("N") ? 0 : 1;

            var lopd = string.IsNullOrEmpty(cliente.TIPOTARIFA) ||
                cliente.TIPOTARIFA.Trim().Equals("No") ||
                cliente.XCLIE_IDCLIENTEFACT.Trim().Equals("Si") ? 0 : 1;

            DateTime? fechaAlta = null;
            DateTime fechaAux;
            if (localAuxiliar != null)
            {
                if (DateTime.TryParse(localAuxiliar.FechaAlta.ToString(), out fechaAux))
                    fechaAlta = new DateTime(fechaAux.Year, fechaAux.Month, fechaAux.Day, fechaAux.Hour, fechaAux.Minute, fechaAux.Second);
            }

            if (string.IsNullOrEmpty(fechaAlta.ToString()))
            {
                if (DateTime.TryParse(cliente.FIS_PROVINCIA, out fechaAux))
                    fechaAlta = new DateTime(fechaAux.Year, fechaAux.Month, fechaAux.Day);
            }               

            var tarjeta = cliente.FIS_FAX ?? string.Empty;

            var telefono = cliente.PER_TELEFONO?.Trim() ??
                cliente.FIS_TELEFONO?.Trim() ??
                string.Empty;

            var direccion = string.Empty;
            if (!string.IsNullOrEmpty(cliente.PER_DIRECCION) && !string.IsNullOrWhiteSpace(cliente.PER_DIRECCION))
            {
                direccion = cliente.PER_DIRECCION.Trim();
                if (!string.IsNullOrEmpty(cliente.PER_CODPOSTAL) && !string.IsNullOrWhiteSpace(cliente.PER_CODPOSTAL))
                    direccion += $" - {cliente.PER_CODPOSTAL.Trim()}";
                if (!string.IsNullOrEmpty(cliente.PER_POBLACION) && !string.IsNullOrWhiteSpace(cliente.PER_POBLACION))
                    direccion += $" - {cliente.PER_POBLACION.Trim()}";
                if (!string.IsNullOrEmpty(cliente.PER_PROVINCIA) && !string.IsNullOrWhiteSpace(cliente.PER_PROVINCIA))
                    direccion += $" ({cliente.PER_PROVINCIA.Trim()})";
            }

            var nombre = string.Empty;
            if (!string.IsNullOrEmpty(cliente.PER_NOMBRE) && !string.IsNullOrWhiteSpace(cliente.PER_NOMBRE))
                nombre = cliente.PER_NOMBRE.Trim().Strip();

            var trabajador = farmaticService.Vendedores.GetOneOrDefaultById(cliente.XVEND_IDVENDEDOR)?.NOMBRE ?? string.Empty;

            var puntos = farmaticService.Clientes.GetTotalPuntosById(cliente.IDCLIENTE);

            return new ClienteDto
            {
                FechaNacimiento = fechaNacimiento,
                FechaAlta = fechaAlta,
                Email = email,
                Movil = movil,
                Direccion = direccion,
                Nombre = nombre,
                Sexo = sexo,
                Telefono = telefono,
                Tarjeta = tarjeta,
                Trabajador = trabajador,
                Puntos = puntos,
                Baja = baja,
                Lopd = lopd
            };
        }

        public static Fisiotes.Models.Cliente GenerarClienteModel(FarmaticService farmaticService, Cliente cliente, bool hasSexo)
        {
            var localDestinatarios = farmaticService.Destinatarios.GetByCliente(cliente.IDCLIENTE);
            var movil = string.Empty;
            var email = string.Empty;
            if (localDestinatarios.Any())
            {
                movil = localDestinatarios.First().TlfMovil?.Trim() ?? movil;
                email = localDestinatarios.First().Email?.Trim() ?? email;
            }

            var fechaNacimiento = default(long);
            var sexo = string.Empty;
            if (hasSexo)
            {
                var localAuxiliar = farmaticService.Clientes.GetAuxiliarById<ClienteAuxWithSexo>(cliente.IDCLIENTE);
                if (localAuxiliar != null)
                {
                    fechaNacimiento = Convert.ToInt64(localAuxiliar.FechaNac?.ToString("yyyyMMdd"));
                    sexo = localAuxiliar.Sexo == "V" ? "Hombre"
                        : localAuxiliar.Sexo == "M" ? "Mujer"
                        : string.Empty;
                }
            }
            else
            {
                var localAuxiliar = farmaticService.Clientes.GetAuxiliarById<ClienteAux>(cliente.IDCLIENTE);
                if (localAuxiliar != null)
                    fechaNacimiento = Convert.ToInt64(localAuxiliar.FechaNac?.ToString("yyyyMMdd"));
            }

            // Si sexo aún no tiene valor, le establecemos uno.
            if (sexo == string.Empty && cliente.FIS_NOMBRE != null)
                sexo = cliente.FIS_NOMBRE;

            var baja = string.IsNullOrEmpty(cliente.FIS_NIF) ||
                cliente.FIS_NIF.Trim().Equals("No") ||
                cliente.FIS_NIF.Trim().Equals("N") ? false : true;

            var lopd = string.IsNullOrEmpty(cliente.TIPOTARIFA) ||
                cliente.TIPOTARIFA.Trim().Equals("No") ||
                cliente.XCLIE_IDCLIENTEFACT.Trim().Equals("Si") ? true : false;

            DateTime? fechaAlta = null;
            if (DateTime.TryParse(cliente.FIS_PROVINCIA, out DateTime fechaAux))
                fechaAlta = new DateTime(fechaAux.Year, fechaAux.Month, fechaAux.Day);

            var tarjeta = cliente.FIS_FAX ?? string.Empty;

            var telefono = cliente.PER_TELEFONO?.Trim() ??
                cliente.FIS_TELEFONO?.Trim() ??
                string.Empty;

            var direccion = string.Empty;
            if (!string.IsNullOrEmpty(cliente.PER_DIRECCION) && !string.IsNullOrWhiteSpace(cliente.PER_DIRECCION))
            {
                direccion = cliente.PER_DIRECCION.Trim();
                if (!string.IsNullOrEmpty(cliente.PER_CODPOSTAL) && !string.IsNullOrWhiteSpace(cliente.PER_CODPOSTAL))
                    direccion += $" - {cliente.PER_CODPOSTAL.Trim()}";
                if (!string.IsNullOrEmpty(cliente.PER_POBLACION) && !string.IsNullOrWhiteSpace(cliente.PER_POBLACION))
                    direccion += $" - {cliente.PER_POBLACION.Trim()}";
                if (!string.IsNullOrEmpty(cliente.PER_PROVINCIA) && !string.IsNullOrWhiteSpace(cliente.PER_PROVINCIA))
                    direccion += $" ({cliente.PER_PROVINCIA.Trim()})";
            }

            var nombre = string.Empty;
            if (!string.IsNullOrEmpty(cliente.PER_NOMBRE) && !string.IsNullOrWhiteSpace(cliente.PER_NOMBRE))
                nombre = cliente.PER_NOMBRE.Trim().Strip();

            var trabajador = farmaticService.Vendedores.GetOneOrDefaultById(cliente.XVEND_IDVENDEDOR)?.NOMBRE ?? string.Empty;

            var puntos = farmaticService.Clientes.GetTotalPuntosById(cliente.IDCLIENTE);

            var dni = cliente.PER_NIF.Strip();

            return new Fisiotes.Models.Cliente
            {
                fecha_nacimiento = fechaNacimiento,
                fechaAlta = fechaAlta,
                email = email,
                movil = movil,
                direccion = direccion,
                apellidos = nombre,
                sexo = sexo,
                telefono = telefono,
                tarjeta = tarjeta,
                nombre_tra = trabajador,
                puntos = puntos,
                baja = baja,
                lopd = lopd,
                dni = cliente.IDCLIENTE
            };
        }

        public static Fisiotes.Models.Medicamento GenerarMedicamento(FarmaticService farmatic, ConsejoService consejo, ArticuloWithIva articulo)
        {
            var familia = farmatic.Familias.GetById(articulo.XFam_IdFamilia)?.Descripcion
                ?? string.Empty;

            var superFamilia = !string.IsNullOrEmpty(familia)
                    ? farmatic.Familias.GetSuperFamiliaDescripcionByFamilia(familia) ?? string.Empty
                    : string.Empty;

            var fechaUltimaCompra = articulo.FechaUltimaEntrada;
            var fechaUltimaVenta = articulo.FechaUltimaSalida;
            var precio = articulo.Pvp;
            var pcoste = articulo.Puc;
            var pvpsIva = Math.Round(articulo.Pvp * 100 / (articulo.Iva + 100), 2);
            var stock = articulo.StockActual;
            var stockMinimo = articulo.StockMinimo;
            var stockMaximo = articulo.StockMaximo;
            var descripcion = articulo.Descripcion.Strip();
            var baja = articulo.Baja;
            var activo = !baja;
            var fechaCaducidad = articulo.FechaCaducidad;

            var codigoBarra = farmatic.Sinonimos.GetOneOrDefaultByArticulo(articulo.IdArticu)?.Sinonimo
                ?? COD_BARRAS_DEFAULT;

            var proveedor = farmatic.Proveedores.GetOneOrDefaultByCodigoNacional(articulo.IdArticu)?.FIS_NOMBRE
                ?? string.Empty;

            var nombreLaboratorio = GetNombreLaboratorioFromLocalOrDefault(
                farmatic, consejo, articulo.Laboratorio, LABORATORIO_DEAFULT);

            var esperara = default(Consejo.Models.Esperara); // consejoService.Esperas.Get(articulo.IdArticu);
            var presentacion = esperara?.PRESENTACION ?? string.Empty;

            var descripcionHtml = string.Empty;
            var textos = new List<string>(); //consejoService.Esperas.GetTextos(articulo.IdArticu);
            foreach (var texto in textos)
            {
                if (string.IsNullOrEmpty(descripcionHtml))
                    descripcionHtml = texto;
                descripcionHtml += $@" <br> {texto}";
            }
            descripcionHtml = descripcionHtml.Length < 30000
                ? descripcionHtml.Replace(Environment.NewLine, "<br>").Replace("\0", string.Empty).Strip()
                : string.Empty;

            return new Fisiotes.Models.Medicamento
            {
                cod_barras = codigoBarra.Strip(),
                cod_nacional = articulo.IdArticu.Strip(),
                familia = familia.Strip(),
                superFamilia = superFamilia.Strip(),
                nombre = descripcion.Strip(),
                precio = Convert.ToSingle(precio),
                nombre_laboratorio = nombreLaboratorio.Strip(),
                laboratorio = articulo.Laboratorio.Strip(),
                proveedor = proveedor.Strip(),
                pvpSinIva = Convert.ToSingle(pvpsIva),
                iva = Convert.ToInt32(articulo.Iva),
                stock = stock,
                puc = Convert.ToSingle(pcoste),
                stockMinimo = stockMinimo,
                stockMaximo = stockMaximo,
                presentacion = presentacion.Strip(),
                descripcion = descripcion.Strip(),
                descripcionTienda = descripcionHtml.Strip(),
                activoPrestashop = activo,
                fechaCaducidad = fechaCaducidad,
                fechaUltimaCompra = fechaUltimaCompra,
                fechaUltimaVenta = fechaUltimaVenta,
                baja = baja
            };
        }

        public static string GetNombreLaboratorioFromLocalOrDefault(FarmaticService farmaticService, ConsejoService consejoService, string codigo, string byDefault = "")
        {
            var nombreLaboratorio = byDefault;
            if (!string.IsNullOrEmpty(codigo?.Trim()) && !string.IsNullOrWhiteSpace(codigo))
            {
                var laboratorioDb = default(Consejo.Models.Labor); //consejoService.Laboratorios.Get(codigo);
                if (laboratorioDb == null)
                {
                    var laboratorioLocal =
                        farmaticService.Laboratorios.GetById(codigo);
                    nombreLaboratorio = laboratorioLocal?.Nombre ?? byDefault;
                }
                else nombreLaboratorio = laboratorioDb.NOMBRE;
            }
            else nombreLaboratorio = byDefault;
            return nombreLaboratorio;
        }
    }
}