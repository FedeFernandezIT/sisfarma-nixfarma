using System;
using Sisfarma.RestClient;
using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using Sisfarma.Sincronizador.Infrastructure.Fisiotes;

namespace Sisfarma.Sincronizador.Unycop.Infrastructure.ExternalServices.Sisfarma
{
    public class EncargosExternalService : FisiotesExternalService, IEncargosExternalService
    {
        public EncargosExternalService(IRestClient restClient, FisiotesConfig config) 
            : base(restClient, config)
        { }

        public bool Exists(int encargo)
        {
            throw new NotImplementedException();
        }

        public Encargo GetByEncargoOrDefault(int encargo)
        {
            throw new NotImplementedException();
        }

        public void Insert(Encargo ee)
        {
            throw new NotImplementedException();
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

        public void Sincronizar(Encargo ee)
        {
            var encargo = new
            {
                idEncargo = ee.idEncargo,
                cod_nacional = ee.cod_nacional,
                nombre = ee.nombre,
                familia = ee.familia.Strip(),
                superFamilia = ee.superFamilia.Strip(),
                cod_laboratorio = ee.cod_laboratorio.Strip(),
                laboratorio = ee.laboratorio.Strip(),
                proveedor = ee.proveedor.Strip(),
                pvp = ee.pvp,
                puc = ee.puc,
                dni = ee.dni,
                fecha = ee.fecha.ToIsoString(),
                trabajador = ee.trabajador,
                unidades = ee.unidades,
                fechaEntrega = ee.fechaEntrega.HasValue ? ee.fechaEntrega.Value.ToIsoString() : DateTime.MinValue.ToIsoString(),
                observaciones = ee.observaciones,                                
                superFamiliaAux = ee.superFamiliaAux.Strip(),
                familiaAux = ee.familiaAux.Strip(),
                cambioClasificacion = ee.cambioClasificacion.ToInteger(),
                categoria = ee.categoria.Strip(),
                subcategoria = ee.subcategoria.Strip()
            };

            _restClient
                .Resource(_config.Encargos.Insert)
                .SendPost(new
                {
                    bulk = new[] { encargo }
                });
        }    

        public void UpdateFechaDeEntrega(DateTime fechaEntrega, long idEncargo)
        {
            throw new NotImplementedException();
        }

        public void UpdateFechaDeRecepcion(DateTime fechaRecepcion, long idEncargo)
        {
            throw new NotImplementedException();
        }
    }
}
