using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class FamiliaSincronizador : TaskSincronizador
    {
        public FamiliaSincronizador(FarmaticService farmatic, FisiotesService fisiotes) 
            : base(farmatic, fisiotes)
        {
        }

        public override void Process() => ProcessFamilia(_farmatic, _fisiotes);

        private void ProcessFamilia(FarmaticService farmatic, FisiotesService fisiotes)
        {
            var familias = farmatic.Familias.Get();
            foreach (var familia in familias)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                //if (!fisiotes.Familias.Exists(familia.Descripcion))
                fisiotes.Familias.Insert(GenerarFamilia(familia.Descripcion.Strip()));
            }
        }

        private Familia GenerarFamilia(string familia)
        {
            return new Familia
            {
                familia = familia,
            };
        }
    }
}
