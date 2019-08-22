using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Consejo;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;

namespace Sisfarma.Sincronizador.Sincronizadores.SuperTypes
{
    public abstract class ControlSincronizador : TaskSincronizador
    {
        protected readonly ConsejoService _consejo;

        public ControlSincronizador(FarmaticService farmatic, FisiotesService fisiotes, ConsejoService consejo)
            : base(farmatic, fisiotes)
        {
            _consejo = consejo ?? throw new ArgumentNullException(nameof(consejo));
        }

        protected void SincronizarMedicamento(FisiotesService fisiotes, Fisiotes.Models.Medicamento remoto, Fisiotes.Models.Medicamento generado)
        {
            if (remoto == null)
                fisiotes.Medicamentos.Insert(generado);                
            else if (HayDiferencias(remoto, generado))
                fisiotes.Medicamentos.Update(generado, withSqlExtra: true);
            else
                fisiotes.Medicamentos.Update(generado);            
        }


        protected virtual bool HayDiferencias(Fisiotes.Models.Medicamento remoto, Fisiotes.Models.Medicamento generado)
        {
            return
                generado.nombre != remoto.nombre ||
                generado.precio != remoto.precio ||
                generado.laboratorio != remoto.laboratorio ||
                generado.iva != remoto.iva ||
                generado.stock != remoto.stock ||
                generado.presentacion != remoto.presentacion ||
                generado.descripcion != remoto.descripcion;
        }
    }
}
