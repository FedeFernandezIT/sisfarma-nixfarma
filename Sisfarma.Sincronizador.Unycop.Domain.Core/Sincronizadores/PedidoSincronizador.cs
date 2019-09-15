using System;
using System.Collections.Generic;
using System.Linq;
using Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

using FAR = Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using SF = Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class PedidoSincronizador : DC.PedidoSincronizador
    {
        private readonly ICategoriaRepository _categoriaRepository;
        private readonly ILaboratorioRepository _laboratorioRepository;
        private readonly IFamiliaRepository _familiaRepository;

        public PedidoSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes)
            : base(farmacia, fisiotes)
        {
            _categoriaRepository = new CategoriaRepository();
            _laboratorioRepository = new LaboratorioRepository();
            _familiaRepository = new FamiliaRepository();
        }

        public override void PreSincronizacion()
        {
            base.PreSincronizacion();
        }

        public override void Process()
        {
            var repository = _farmacia.Recepciones as RecepcionRespository;
            var pedidos = (_lastPedido == null)
                ? repository.GetAllByYearAsDTO(_anioInicio)
                : repository.GetAllByDateAsDTO(_lastPedido.fechaPedido ?? DateTime.MinValue);

            if (!pedidos.Any())
            {
                if (_anioInicio < DateTime.Now.Year)
                {
                    _anioInicio++;
                    _lastPedido = null;
                }

                return;
            }

            var pedidosAgrupados = pedidos.GroupBy(k => new { k.Fecha.Year, k.Empresa, k.Pedido })
                        .ToDictionary(
                            k => new RecepcionCompositeKey { Anio = k.Key.Year, Empresa = k.Key.Empresa, Pedido = k.Key.Pedido },
                            v => v.ToList());

            foreach (var pedido in pedidosAgrupados)
            {
                var fecha = pedido.Value.First().Fecha; // a la vuelta preguntamos por > fecha
                var proveedorPedido = pedido.Value.First().Proveedor.HasValue ?
                    _farmacia.Proveedores.GetOneOrDefaultById(pedido.Value.First().Proveedor.Value) : null;

                var numeroPedido = pedido.Key.Pedido;
                var numeroPedidoSerial = numeroPedido.ToString().PadLeft(6, '0');
                var empresa = pedido.Key.Empresa;
                var empresaSerial = empresa == "EMP1" ? "00001" : "00002";
                var anio = pedido.Key.Anio;
                var identity = long.Parse($"{anio}{numeroPedidoSerial}{empresaSerial}");

                var totales = repository.GetTotalesByPedidoAsDTO(anio, numeroPedido, empresa);
                var recepcion = new FAR.Recepcion
                {
                    Id = identity,
                    Pedido = numeroPedido,
                    Fecha = fecha,
                    Lineas = totales.Lineas,
                    ImportePVP = totales.PVP,
                    ImportePUC = totales.PUC,
                    Proveedor = proveedorPedido
                };

                var detalle = new List<RecepcionDetalle>();
                foreach (var item in pedido.Value)
                {
                    var farmaco = (_farmacia.Farmacos as FarmacoRespository).GetOneOrDefaultById(item.Farmaco);
                    if (farmaco != null)
                    {
                        var recepcionDetalle = new RecepcionDetalle()
                        {
                            Linea = item.Linea,
                            RecepcionId = identity,
                            Cantidad = item.Recibido,
                            Recepcion = recepcion
                        };

                        var pvp = item.ImportePvp;
                        var puc = item.ImportePuc != 0 ? item.ImportePuc : farmaco.PUC;

                        var proveedor = _farmacia.Proveedores.GetOneOrDefaultByCodigoNacional(farmaco.Codigo)
                                ?? (item.Proveedor.HasValue ? _farmacia.Proveedores.GetOneOrDefaultById(item.Proveedor.Value)
                                : null);

                        var categoria = _categoriaRepository.GetOneOrDefaultById(farmaco.Codigo);

                        FAR.Familia familia = null;
                        FAR.Familia superFamilia = null;
                        if (string.IsNullOrWhiteSpace(farmaco.SubFamilia))
                        {
                            familia = new FAR.Familia { Nombre = string.Empty };
                            superFamilia = _familiaRepository.GetOneOrDefaultById(farmaco.Familia)
                                ?? new FAR.Familia { Nombre = string.Empty };
                        }
                        else
                        {
                            familia = _familiaRepository.GetSubFamiliaOneOrDefault(farmaco.Familia, farmaco.SubFamilia)
                                ?? new FAR.Familia { Nombre = string.Empty };
                            superFamilia = _familiaRepository.GetOneOrDefaultById(farmaco.Familia)
                                ?? new FAR.Familia { Nombre = string.Empty };
                        }

                        var laboratorio = !farmaco.Laboratorio.HasValue ? new Laboratorio { Codigo = string.Empty, Nombre = "<Sin Laboratorio>" }
                            : _laboratorioRepository.GetOneOrDefaultByCodigo(farmaco.Laboratorio.Value, farmaco.Clase, farmaco.ClaseBot)
                                ?? new Laboratorio { Codigo = string.Empty, Nombre = "<Sin Laboratorio>" };

                        recepcionDetalle.Farmaco = new Farmaco
                        {
                            Id = farmaco.Id,
                            Codigo = item.Farmaco,
                            PrecioCoste = puc,
                            Proveedor = proveedor,
                            Categoria = categoria,
                            Familia = familia,
                            SuperFamilia = superFamilia,
                            Laboratorio = laboratorio,
                            Denominacion = farmaco.Denominacion,
                            Precio = pvp
                        };

                        detalle.Add(recepcionDetalle);
                        _sisfarma.Pedidos.Sincronizar(GenerarLineaDePedido(recepcionDetalle));
                    }
                }

                if (detalle.Any())
                {
                    var pedidoCabecera = GenerarPedido(recepcion);
                    _sisfarma.Pedidos.Sincronizar(pedidoCabecera);

                    _lastPedido = pedidoCabecera;
                }
            }
        }

        internal class RecepcionCompositeKey
        {
            internal int Anio { get; set; }

            internal string Empresa { get; set; }

            internal long Pedido { get; set; }
        }

        private LineaPedido GenerarLineaDePedido(FAR.RecepcionDetalle detalle)
        {
            return new LineaPedido
            {
                idPedido = detalle.RecepcionId,
                idLinea = detalle.Linea,
                fechaPedido = detalle.Recepcion.Fecha,
                cod_nacional = long.TryParse(detalle.Farmaco.Codigo.TrimStart('0'), out var codigoNacional) ? codigoNacional : 0L,
                descripcion = detalle.Farmaco.Denominacion,
                familia = detalle.Farmaco.Familia?.Nombre ?? FAMILIA_DEFAULT,
                superFamilia = detalle.Farmaco.SuperFamilia?.Nombre ?? FAMILIA_DEFAULT,
                categoria = detalle.Farmaco.Categoria?.Nombre ?? string.Empty,
                cantidad = detalle.Cantidad,
                pvp = detalle.Farmaco?.Precio ?? 0m,
                puc = detalle.Farmaco?.PrecioCoste ?? 0m,
                cod_laboratorio = detalle.Farmaco?.Laboratorio?.Codigo ?? "0",
                laboratorio = detalle.Farmaco?.Laboratorio?.Nombre ?? LABORATORIO_DEFAULT,
                proveedor = detalle.Farmaco?.Proveedor?.Nombre ?? string.Empty
            };
        }

        private SF.Pedido GenerarPedido(FAR.Recepcion recepcion)
        {
            return new SF.Pedido
            {
                idPedido = recepcion.Id,
                fechaPedido = recepcion.Fecha,
                hora = DateTime.Now,
                numLineas = recepcion.Lineas,
                importePvp = recepcion.ImportePVP,
                importePuc = recepcion.ImportePUC,
                idProveedor = recepcion.Proveedor?.Id.ToString() ?? "0",
                proveedor = recepcion.Proveedor?.Nombre ?? string.Empty,
                trabajador = string.Empty
            };
        }
    }
}