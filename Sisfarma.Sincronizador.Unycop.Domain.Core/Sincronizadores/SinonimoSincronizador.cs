using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Data;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class SinonimoSincronizador : DC.SinonimoSincronizador
    {
        public SinonimoSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes)
            : base(farmacia, fisiotes)
        { }

        public override void LoadConfiguration()
        {
        }

        public override void PreSincronizacion()
        {
        }

        public override void Process()
        {
            // _isEmpty se carga en PreSincronizacion()
            if (!_isEmpty && _horariosDeVaciamiento.Any(x => x.Equals(DateTime.Now.ToString("HHmm"))))
            {
                _sisfarma.Sinonimos.Empty();
                _isEmpty = _sisfarma.Sinonimos.IsEmpty();
            }

            if (_isEmpty)
            {
                var sinonimos = _farmacia.Sinonimos.GetAll();

                for (int i = 0; i < sinonimos.Count(); i += _batchSize)
                {
                    Task.Delay(1);

                    _cancellationToken.ThrowIfCancellationRequested();

                    var items = sinonimos
                        .Skip(i)
                        .Take(_batchSize)
                            .Select(x => new Sinonimo
                            {
                                cod_barras = x.CodigoBarra,
                                cod_nacional = x.CodigoNacional
                            }).ToList();

                    _sisfarma.Sinonimos.Sincronizar(items);
                    // 1er lote pregunta
                    if (_isEmpty)
                        _isEmpty = _sisfarma.Sinonimos.IsEmpty();
                }
            }
        }
    }
}