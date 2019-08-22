using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class ListaFechaSincronizador : TaskSincronizador
    {
        private const int BATCH_SIZE = 1000;

        private readonly int _listaDeArticulo;

        public ListaFechaSincronizador(FarmaticService farmatic, FisiotesService fisiotes, int listaDeArticulo) 
            : base(farmatic, fisiotes)
        {
            _listaDeArticulo = listaDeArticulo;
        }

        public override void Process() => ProcessListasFechas(_farmatic, _fisiotes, _listaDeArticulo);

        public void ProcessListasFechas(FarmaticService farmatic, FisiotesService fisiotes, int listaDeArticulo)
        {
            var listas = farmatic.ListasArticulos.GetByFechaExceptList(listaDeArticulo);
            foreach (var lista in listas)
            {
                _cancellationToken.ThrowIfCancellationRequested();

                var listaRemote = fisiotes.Listas.GetOneOrDefault(lista.IdLista);
                fisiotes.Listas.InsertOrUpdate(new Fisiotes.Models.Lista
                {
                    cod = lista.IdLista,
                    lista = lista.Descripcion.Strip()
                });

                fisiotes.Listas.DeArticulos.Delete(lista.IdLista);
                var articulos = farmatic.ListasArticulos.GetArticulosByLista(lista.IdLista);
                if (articulos.Any())
                {
                    for (int i = 0; i < articulos.Count; i += BATCH_SIZE)
                    {
                        _cancellationToken.ThrowIfCancellationRequested();

                        var items = articulos
                            .Skip(i)
                            .Take(BATCH_SIZE)
                            .Select(x => new Fisiotes.Models.ListaArticulo
                            {
                                cod_lista = x.XItem_IdLista,
                                cod_articulo = Convert.ToInt32(x.XItem_IdArticu)
                            }).ToList();

                        fisiotes.Listas.DeArticulos.Insert(items);
                    }
                }                
            }
        }
    }
}
