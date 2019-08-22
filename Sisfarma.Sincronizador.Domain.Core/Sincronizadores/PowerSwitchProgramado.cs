using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class PowerSwitchProgramado : PowerSwitch
    {
        public PowerSwitchProgramado(ISisfarmaService sisfarma)
            : base(sisfarma)
        { }

        public override void Process() => ProcessPowerSwitch();

        private void ProcessPowerSwitch()
        {
            var encedido = _sisfarma.Programacion.GetProgramacionOrDefault(Programacion.Encendido);
            var apagado = _sisfarma.Programacion.GetProgramacionOrDefault(Programacion.Apagado);

            if (encedido == null || apagado == null)
                return;


            var turnoMatutino = new Turno(encedido.horaM, apagado.horaM);
            var turnoTarde = new Turno(encedido.horaT, apagado.horaT);

            var debeEstarPrendido = turnoMatutino.EstaEnHorarioDeAtencion() || turnoTarde.EstaEnHorarioDeAtencion();

            if (!EstaEncendido && debeEstarPrendido)            
                Encender();            
            else if (EstaEncendido && turnoMatutino.EstaProgramado && turnoTarde.EstaProgramado && !debeEstarPrendido)            
                Apagar();            
        }

        protected override void Encender()
        {
            base.Encender();
            _sisfarma.Configuraciones.Update(FIELD_ENCENDIDO, Programacion.Encendido);
        }

        protected override void Apagar()
        {
            base.Apagar();
            _sisfarma.Configuraciones.Update(FIELD_ENCENDIDO, Programacion.Apagado);
        }
        

        public class Turno
        {
            public int? Apertura { get; set; }
            public int? Cierre { get; set; }

            public bool EstaProgramado => Apertura.HasValue && Cierre.HasValue;

            public Turno(string apertura, string cierre)
            {
                Apertura = GetHoraMinutoOrDefault(apertura);
                Cierre = GetHoraMinutoOrDefault(cierre);
            }

            public bool EstaEnHorarioDeAtencion()
            {
                if (!EstaProgramado)
                    return false;

                var horario = DateTime.Now.ToTimeInteger();
                return Apertura.Value <= horario && horario < Cierre.Value;
            }

            private int? GetHoraMinutoOrDefault(string horario)
            {
                if (TimeSpan.TryParse(horario, out var hhmm))
                    return DateTime.Today.Add(hhmm).ToTimeInteger();
                return null;
            }            
        }
    }
}
