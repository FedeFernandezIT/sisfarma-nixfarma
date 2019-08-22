using System.Linq;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class ProveedorHistorialSincronizador : DC.ProveedorHistorialSincronizador
    {
        public ProveedorHistorialSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        { }

        public override void Process()
        {
            // _fechaMax se carga en PreSincronizador()
            var histricos = _farmacia.Recepciones.GetAllHistoricosByFecha(_fechaMax);

            for (int i = 0; i < histricos.Count(); i += _batchSize)
            {
                Task.Delay(1);

                _cancellationToken.ThrowIfCancellationRequested();

                var items = histricos
                    .Skip(i)
                    .Take(_batchSize)
                        .Select(x => new ProveedorHistorial
                        {
                            idProveedor = x.Id.ToString(),
                            cod_nacional = x.FarmacoId.ToString(),
                            fecha = x.Fecha,
                            puc = x.PUC
                        }).ToList();

                _sisfarma.Proveedores.Sincronizador(items);
            }
        }
    }
}
