using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class CategoriaSincronizador : TaskSincronizador
    {
        private const string PADRE_DEFAULT = @"<SIN PADRE>";

        public CategoriaSincronizador(FarmaticService farmatic, FisiotesService fisiotes) 
            : base(farmatic, fisiotes)
        {
        }

        public override void Process() => ProcessCategorias(_farmatic, _fisiotes);

        private void ProcessCategorias(FarmaticService farmatic, FisiotesService fisiotes)
        {
            var familias = farmatic.Familias.GetByDescripcion();
            foreach (var familia in familias)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                var padre = farmatic.Familias.GetSuperFamiliaDescripcionByFamilia($"{familia.Descripcion}") 
                    ?? PADRE_DEFAULT;
                                
                /*if (!fisiotes.Categorias.Exists(familia.Descripcion.Strip(), padre.Strip()))
                {
                    var categoria = fisiotes.Categorias.GetByPadreOrDefault(padre.Strip());
                    var padreId = categoria?.prestashopPadreId;*/
                    fisiotes.Categorias.Insert(new Fisiotes.Models.Categoria
                        {
                            categoria = familia.Descripcion.Strip(),
                            padre = padre.Strip(),
                            prestashopPadreId = null                            
                        });
                //}
            }
        }
    }
}
