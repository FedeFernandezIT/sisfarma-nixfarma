using System;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Consejo;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using static Sisfarma.Sincronizador.Fisiotes.Repositories.ConfiguracionesRepository;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class EncargoSincronizador : TaskSincronizador
    {        
        private const string LABORATORIO_DEFAULT = "<Sin Laboratorio>";
        private const string FAMILIA_DEFAULT = "<Sin Clasificar>";        

        private readonly ConsejoService _consejo;
        private int _anioInicio;
        private Encargo _ultimo;

        public EncargoSincronizador(FarmaticService farmatic, FisiotesService fisiotes, ConsejoService consejo) 
            : base(farmatic, fisiotes)
        {
            _consejo = consejo ?? throw new ArgumentNullException(nameof(consejo));
        }

        public override void LoadConfiguration()
        {
            _anioInicio = ConfiguracionPredefinida[Configuracion.FIELD_ANIO_INICIO]
                .ToIntegerOrDefault(@default: DateTime.Now.Year - 2);
        }

        public override void PreSincronizacion()
        {
            _ultimo = _fisiotes.Encargos.LastOrDefault();
        }

        public override void Process() => ProcessEncargos();

        private void ProcessEncargos()
        {            
            //var ultimo = _fisiotes.Encargos.LastOrDefault();
            var idEncargo = _ultimo?.idEncargo ?? 1;

            var encargos = _farmatic.Encargos.GetAllByContadorGreaterOrEqual(_anioInicio, idEncargo);
            foreach (var encargo in encargos)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                //if (!_fisiotes.Encargos.Exists(encargo.IdContador))                
                    _fisiotes.Encargos.Insert(GenerarEncargo(_farmatic, encargo, _consejo));
                    if (_ultimo == null)
                        _ultimo = new Fisiotes.Models.Encargo();

                    _ultimo.idEncargo = encargo.IdContador;
            }
        }

        private Fisiotes.Models.Encargo GenerarEncargo(FarmaticService farmatic, Farmatic.Models.Encargo encargo, ConsejoService consejo)
        {            
            var nombre = string.Empty;
            var familia = FAMILIA_DEFAULT;
            var superFamilia = FAMILIA_DEFAULT;
            var codLaboratorio = string.Empty;
            var laboratorio = LABORATORIO_DEFAULT;
            var proveedor = string.Empty;
            var pcoste = 0d;
            var precioMed = 0d;

            var dni = !string.IsNullOrEmpty(encargo.XCli_IdCliente)
                ? encargo.XCli_IdCliente.Trim()
                : "0";

            var trabajador = farmatic.Vendedores.GetOneOrDefaultById(Convert.ToInt16(encargo.Vendedor))?.NOMBRE
                ?? string.Empty;

            var codNacional = encargo.XArt_IdArticu?.Trim();
            if (string.IsNullOrWhiteSpace(codNacional))
                codNacional = "0";

            var articulo = farmatic.Articulos.GetOneOrDefaultById(encargo.XArt_IdArticu);
            if (articulo != null)
            {
                nombre = articulo.Descripcion.Strip();
                pcoste = articulo.Puc;
                precioMed = articulo.Pvp;

                familia = farmatic.Familias.GetById(articulo.XFam_IdFamilia)?.Descripcion;
                if (string.IsNullOrEmpty(familia))
                    familia = FAMILIA_DEFAULT;

                superFamilia = !familia.Equals(FAMILIA_DEFAULT)
                    ? farmatic.Familias.GetSuperFamiliaDescripcionByFamilia(familia) ?? FAMILIA_DEFAULT
                    : familia;

                proveedor = farmatic.Proveedores.GetOneOrDefaultByCodigoNacional(articulo.IdArticu)?.FIS_NOMBRE.Strip() ?? string.Empty;

                codLaboratorio = articulo.Laboratorio.Strip() ?? string.Empty;
                laboratorio = GetNombreLaboratorioFromLocalOrDefault(farmatic, consejo, codLaboratorio, LABORATORIO_DEFAULT).Strip();
            }

            return new Fisiotes.Models.Encargo
            {
                idEncargo = encargo.IdContador,
                cod_nacional = codNacional,
                nombre = nombre,
                familia = familia,
                superFamilia = superFamilia,
                cod_laboratorio = codLaboratorio,
                laboratorio = laboratorio,
                proveedor = proveedor,
                pvp = Convert.ToSingle(precioMed),
                puc = Convert.ToSingle(pcoste),
                dni = dni,
                fecha = encargo.IdFecha,
                trabajador = trabajador,
                unidades = encargo.Unidades,
                fechaEntrega = encargo.FechaEntrega,
                observaciones = encargo.Observaciones.Strip()
            };            
        }

        string GetNombreLaboratorioFromLocalOrDefault(FarmaticService farmaticService, ConsejoService consejoService, string codigo, string byDefault = "")
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
