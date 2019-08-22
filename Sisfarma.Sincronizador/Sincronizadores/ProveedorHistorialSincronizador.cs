using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Farmatic.DTO.Recepciones;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class ProveedorHistorialSincronizador : TaskSincronizador
    {
        private readonly int _batchSize;

        private DateTime _fechaMax;

        public ProveedorHistorialSincronizador(FarmaticService farmatic, FisiotesService fisiotes) 
            : base(farmatic, fisiotes)
        {
            _batchSize = 1000;
        }

        public override void Process() => ProcessProveedorHistorial();

        public override void PreSincronizacion()
        {
            _fechaMax = _fisiotes.Proveedores.GetFechaMaximaDeHistorico() ?? new DateTime(DateTime.Now.Year - 2, 1, 1);
        }

        private void ProcessProveedorHistorial()
        {
            //var fechaMax = _fisiotes.Proveedores.GetFechaMaximaDeHistorico();            
            /*var recepciones = _fechaMax.HasValue
                ? _farmatic.Recepciones.GetGroupGreaterThanByFecha(_fechaMax.Value)
                : _farmatic.Recepciones.GetGroupGreaterOrEqualByFecha(new DateTime(DateTime.Now.Year - 2, 1, 1));*/

            var recepciones = _farmatic.Recepciones.GetGroupGreaterThanByFecha(_fechaMax);
                
            for (int i = 0; i < recepciones.Count(); i += _batchSize)
            {
                Task.Delay(1);

                _cancellationToken.ThrowIfCancellationRequested();

                var items = recepciones
                    .Skip(i)
                    .Take(_batchSize)
                        .Select(x => new ProveedorHistorial
                        {
                            idProveedor = x.XProv_IdProveedor,
                            cod_nacional = x.XArt_IdArticu,
                            fecha = x.Hora,
                            puc = (decimal) x.ImportePuc
                        }).ToList();

                _fisiotes.Proveedores.InsertHistorico(items);
            }
        }
    }
}
