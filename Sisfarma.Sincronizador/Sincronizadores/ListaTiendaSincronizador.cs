using Sisfarma.Sincronizador.Consejo;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Helpers;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class ListaTiendaSincronizador : ControlSincronizador
    {        
        private readonly int _listaDeArticulo;

        public ListaTiendaSincronizador(FarmaticService farmatic, FisiotesService fisiotes, ConsejoService consejo, int listaDeArticulo)
            : base(farmatic, fisiotes, consejo)
        {            
            _listaDeArticulo = listaDeArticulo;
        }

        public override void Process() => ProcessListaTienda(_farmatic, _fisiotes, _consejo, _listaDeArticulo);

        private void ProcessListaTienda(FarmaticService farmatic, FisiotesService fisiotes, ConsejoService consejo, int listaDeArticulo)
        {
            if (listaDeArticulo <= 0)
                return;

            var lista = farmatic.ListasArticulos.GetOneOrDefault(listaDeArticulo);
            if (lista != null)
            {
                fisiotes.Listas.InsertOrUpdate(new Fisiotes.Models.Lista
                {
                    cod = lista.IdLista,
                    lista = lista.Descripcion.Strip()
                });

                fisiotes.Listas.DeArticulos.Delete(lista.IdLista);
                var articulos = farmatic.ListasArticulos.GetArticulosByLista(lista.IdLista);
                foreach (var articulo in articulos)
                {
                    _cancellationToken.ThrowIfCancellationRequested();

                    fisiotes.Listas.DeArticulos.Insert(new Fisiotes.Models.ListaArticulo
                    {
                        cod_lista = articulo.XItem_IdLista,
                        cod_articulo = Convert.ToInt32(articulo.XItem_IdArticu)
                    });

                    var awi = farmatic.ListasArticulos.GetArticuloWithIva(listaDeArticulo, articulo.XItem_IdArticu);

                    var medicamentoGenerado = Generator.GenerarMedicamento(farmatic, consejo, awi);
                    var medicamento = fisiotes.Medicamentos.GetOneOrDefaultByCodNacional(awi.IdArticu);

                    SincronizarMedicamento(fisiotes, medicamento, medicamentoGenerado);
                }

                farmatic.ListasArticulos.Update(listaDeArticulo);
            }
        }
    }
}
