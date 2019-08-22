using System;
using System.Linq;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class SinonimoSincronizador : TaskSincronizador
    {
        private readonly string[] _horariosDeVaciamiento;
        private readonly int _batchSize;

        private bool _isEmpty;

        public SinonimoSincronizador(FarmaticService farmatic, FisiotesService fisiotes)
            : base(farmatic, fisiotes)
        {
            _horariosDeVaciamiento = new[] { "1230", "1730", "1930" };
            _batchSize = 1000;
        }

        public override void Process() => ProcessSinonimos(_farmatic, _fisiotes);

        public override void PreSincronizacion()
        {
            _isEmpty = _fisiotes.Sinonimos.IsEmpty();
        }

        public void ProcessSinonimos(FarmaticService farmaticService, FisiotesService fisiotesService)
        {
            if (!_isEmpty && _horariosDeVaciamiento.Any(x => x.Equals(DateTime.Now.ToString("HHmm"))))
            {
                fisiotesService.Sinonimos.Empty();
                _isEmpty = _fisiotes.Sinonimos.IsEmpty();
            }

            if (_isEmpty)
            {
                var sinonimos = farmaticService.Sinonimos.GetAll();

                for (int i = 0; i < sinonimos.Count; i += _batchSize)
                {
                    Task.Delay(1);

                    _cancellationToken.ThrowIfCancellationRequested();

                    var items = sinonimos
                        .Skip(i)
                        .Take(_batchSize)
                            .Select(x => new Sinonimo
                            {
                                cod_barras = x.Sinonimo.Strip(),
                                cod_nacional = x.IdArticu.Strip()
                            }).ToList();

                    fisiotesService.Sinonimos.Insert(items);
                    // 1er lote pregunta
                    if (_isEmpty)
                        _isEmpty = _fisiotes.Sinonimos.IsEmpty();
                }
            }
        }
    }
}