using System;
using System.Linq;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Helpers;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class ListaSincronizador : TaskSincronizador
    {
        private const int BATCH_SIZE = 1000;
        private int _codActual;

        public ListaSincronizador(FarmaticService farmatic, FisiotesService fisiotes) 
            : base(farmatic, fisiotes)
        {
        }

        public override void PreSincronizacion()
        {
            _codActual = _fisiotes.Listas.GetCodPorDondeVoyOrDefault()?.cod ?? -1;
        }

        public override void Process() => ProcessListas(_farmatic, _fisiotes);

        private void ProcessListas(FarmaticService farmatic, FisiotesService fisiotes)
        {
            var listas = farmatic.ListasArticulos.GetByIdGreaterThan(_codActual);
            foreach (var lista in listas)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                //fisiotes.Listas.ResetPorDondeVoy();
                fisiotes.Listas.InsertOrUpdate(new Fisiotes.Models.Lista
                {
                    cod = lista.IdLista,
                    lista = lista.Descripcion.Strip()                    
                });

                _codActual = lista.IdLista;

                var articulos = farmatic.ListasArticulos.GetArticulosByLista(lista.IdLista);
                if (articulos.Any())
                {
                    fisiotes.Listas.DeArticulos.Delete(lista.IdLista);                    

                    for (int i = 0; i < articulos.Count; i += BATCH_SIZE)
                    {
                        Task.Delay(1);

                        var items = articulos
                            .Skip(i)
                            .Take(BATCH_SIZE)
                            .Select(x => new Fisiotes.Models.ListaArticulo
                            {
                                cod_lista = x.XItem_IdLista,
                                cod_articulo = x.XItem_IdArticu.ToIntegerOrDefault(-1)
                            }).ToList();

                        fisiotes.Listas.DeArticulos.Insert(items);
                    }
                }
            }

            _codActual = -1;
            //fisiotes.Listas.ResetPorDondeVoy();
        }
    }
}
