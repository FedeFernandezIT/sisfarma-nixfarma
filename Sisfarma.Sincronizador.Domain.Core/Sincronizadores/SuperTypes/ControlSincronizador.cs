using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes
{
    public abstract class ControlSincronizador : TaskSincronizador
    {
        public ControlSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes)
            : base(farmacia, fisiotes)
        { }

        protected void SincronizarMedicamento(ISisfarmaService fisiotes, Medicamento remoto, Medicamento generado)
        {
            if (remoto == null)
                fisiotes.Medicamentos.Insert(generado);                
            else if (HayDiferencias(remoto, generado))
                fisiotes.Medicamentos.Update(generado, withSqlExtra: true);
            else
                fisiotes.Medicamentos.Update(generado);            
        }

        protected virtual bool HayDiferencias(Medicamento remoto, Medicamento generado)
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
