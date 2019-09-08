using System;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;
using FAR = Sisfarma.Sincronizador.Domain.Entities.Farmacia;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class EncargoSincronizador : DC.EncargoSincronizador
    {
        protected const string TIPO_CLASIFICACION_DEFAULT = "Familia";
        protected const string TIPO_CLASIFICACION_CATEGORIA = "Categoria";

        private string _clasificacion;
        private string _verCategorias;

        public EncargoSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes)
            : base(farmacia, fisiotes)
        { }

        public override void LoadConfiguration()
        {
            base.LoadConfiguration();
            _clasificacion = !string.IsNullOrWhiteSpace(ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION])
                ? ConfiguracionPredefinida[Configuracion.FIELD_TIPO_CLASIFICACION]
                : TIPO_CLASIFICACION_DEFAULT;
            _verCategorias = ConfiguracionPredefinida[Configuracion.FIELD_VER_CATEGORIAS];
        }

        public override void PreSincronizacion()
        {
            base.PreSincronizacion();
        }

        public override void Process()
        {
            //_ultimo se carga en PreSincronizacion()
            var idEncargo = _ultimo?.idEncargo ?? 1;

            var encargos = _farmacia.Encargos.GetAllByIdGreaterOrEqual(_anioInicio, idEncargo);
            foreach (var encargo in encargos)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();
                _sisfarma.Encargos.Sincronizar(GenerarEncargo(encargo));

                if (_ultimo == null)
                    _ultimo = new Encargo();

                _ultimo.idEncargo = encargo.Id;
            }
        }

        private Encargo GenerarEncargo(FAR.Encargo encargo)
        {
            var familia = !string.IsNullOrWhiteSpace(encargo.Farmaco.Familia?.Nombre) ? encargo.Farmaco.Familia.Nombre : FAMILIA_DEFAULT;
            var superFamilia = !string.IsNullOrWhiteSpace(encargo.Farmaco.SuperFamilia?.Nombre) ? encargo.Farmaco.SuperFamilia.Nombre : FAMILIA_DEFAULT;

            var categoria = encargo.Farmaco.Categoria?.Nombre;
            if (_verCategorias == "si" && !string.IsNullOrWhiteSpace(categoria) && categoria.ToLower() != "sin categoria" && categoria.ToLower() != "sin categoría")
            {
                if (string.IsNullOrEmpty(superFamilia) || superFamilia == FAMILIA_DEFAULT)
                    superFamilia = categoria;
                else superFamilia = $"{superFamilia} ~~~~~~~~ {categoria}";
            }

            return new Encargo
            {
                idEncargo = encargo.Id,
                cod_nacional = encargo.Farmaco.Codigo,
                nombre = encargo.Farmaco.Denominacion,
                familia = familia,
                superFamilia = superFamilia,

                cambioClasificacion = _clasificacion == TIPO_CLASIFICACION_CATEGORIA,
                cod_laboratorio = encargo.Farmaco.Laboratorio?.Codigo ?? string.Empty,
                laboratorio = encargo.Farmaco.Laboratorio?.Nombre ?? LABORATORIO_DEFAULT,
                proveedor = encargo.Farmaco.Proveedor?.Nombre ?? string.Empty,
                pvp = encargo.Farmaco.Precio,
                puc = encargo.Farmaco.PrecioCoste,
                dni = encargo.Cliente?.Id.ToString() ?? "0",
                fecha = encargo.Fecha,
                fechaEntrega = encargo.FechaEntrega,
                trabajador = encargo.Vendedor?.Nombre ?? string.Empty,
                unidades = encargo.Cantidad,
                observaciones = encargo.Observaciones,
                categoria = encargo.Farmaco.Categoria?.Nombre ?? familia,

                empresa_codigo = encargo.Empresa,
                almacen_codigo = encargo.Almacen,
                idLinea = encargo.Linea
            };
        }
    }
}