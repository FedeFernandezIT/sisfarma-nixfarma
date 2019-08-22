using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System;

namespace Sisfarma.Sincronizador.Infrastructure.Fisiotes
{
    public class EncargosExternalService : FisiotesExternalService, IEncargosExternalService
    {     
        public EncargosExternalService(IRestClient restClient, FisiotesConfig config)
            : base(restClient, config)
        {
         }

        public Encargo LastOrDefault()
        {
            try
            {
                return _restClient
                .Resource(_config.Encargos.Ultimo)
                .SendGet<Encargo>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public bool Exists(int encargo)
        {
            return GetByEncargoOrDefault(encargo) != null;
        }

        public Encargo GetByEncargoOrDefault(int encargo)
        {
            try
            {
                return _restClient
                    .Resource(_config.Encargos.GetByEncargo.Replace("{encargo}", $"{encargo}"))
                    .SendGet<Encargo>();
            }
            catch (RestClientNotFoundException)
            {
                return null;
            }
        }

        public void UpdateFechaDeRecepcion(DateTime fechaRecepcion, long idEncargo)
        {
            var encargo = new
            {
                idEncargo = idEncargo,
                fechaRecepcion = fechaRecepcion.ToIsoString()                
            };

            _restClient
                .Resource(_config.Encargos.Update)
                .SendPost(new
                {
                    bulk = new[] { encargo }
                });
        }

        public void UpdateFechaDeEntrega(DateTime fechaEntrega, long idEncargo)
        {
            var encargo = new
            {
                idEncargo = idEncargo,
                fechaEntrega = fechaEntrega.ToIsoString()
            };

            _restClient
                .Resource(_config.Encargos.Update)
                .SendPost(new
                {
                    bulk = new[] { encargo }
                });
        }
                
                
        public void Insert(Encargo ee)
        {
            var encargo =  new
            {
                idEncargo = ee.idEncargo,
                cod_nacional = ee.cod_nacional,
                nombre = ee.nombre,
                familia = ee.familia,
                superFamilia = ee.superFamilia,
                cod_laboratorio = ee.cod_laboratorio,
                laboratorio = ee.laboratorio,
                proveedor = ee.proveedor,
                pvp = ee.pvp,
                puc = ee.puc,
                dni = ee.dni,
                fecha = ee.fecha.ToIsoString(),
                trabajador = ee.trabajador,
                unidades = ee.unidades,
                fechaEntrega = ee.fechaEntrega.ToIsoString(),
                observaciones = ee.observaciones
            };

            _restClient
                .Resource(_config.Encargos.Insert)
                .SendPost(new
                {
                    bulk = new[] { encargo }
                });
        }

        public void Sincronizar(Encargo encargo)
        {
            throw new NotImplementedException();
        }
    }
}