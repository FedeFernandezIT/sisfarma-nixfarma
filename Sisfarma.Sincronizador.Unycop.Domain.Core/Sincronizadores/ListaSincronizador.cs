using System.Linq;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using DC = Sisfarma.Sincronizador.Domain.Core.Sincronizadores;

namespace Sisfarma.Sincronizador.Unycop.Domain.Core.Sincronizadores
{
    public class ListaSincronizador : DC.ListaSincronizador
    {
        public ListaSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes) 
            : base(farmacia, fisiotes)
        { }

        public override void Process()
        {
            var listas = _farmacia.Listas.GetAllByIdGreaterThan(_codActual);
            foreach (var lista in listas)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();
                
                _sisfarma.Listas.Sincronizar(new Lista { cod = lista.Id, lista = lista.Descripcion });

                _codActual = lista.Id;
                
                if (lista.Farmacos.Any())
                {
                    _sisfarma.Listas.DeArticulos.Delete(lista.Id);

                    for (int i = 0; i < lista.Farmacos.Count; i += BATCH_SIZE)
                    {
                        Task.Delay(1);

                        var items = lista.Farmacos
                            .Skip(i)
                            .Take(BATCH_SIZE)
                            .Select(x => new ListaArticulo
                            {
                                cod_lista = x.ListaId,
                                cod_articulo = x.FarmacoId
                            }).ToList();

                        _sisfarma.Listas.DeArticulos.Sincronizar(items);
                    }
                }
            }

            _codActual = -1;
        }        
    }
}
