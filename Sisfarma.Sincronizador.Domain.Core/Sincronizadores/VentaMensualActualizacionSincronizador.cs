using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores
{
    public class VentaMensualActualizacionSincronizador : TaskSincronizador
    {        
        protected bool _perteneceFarmazul;
        protected string _puntosDeSisfarma;
        protected string _cargarPuntos;
        protected string _fechaDePuntos;
        protected string _soloPuntosConTarjeta;
        protected string _canjeoPuntos;

        protected const string VENDEDOR_DEFAULT = "NO";
        protected const string COD_BARRAS_DEFAULT = "847000";        
        protected const string FAMILIA_DEFAULT = "<Sin Clasificar>";
        protected const string LABORATORIO_DEFAULT = "<Sin Laboratorio>";


        protected readonly int _listaDeArticulo;        
        
        public VentaMensualActualizacionSincronizador(IFarmaciaService farmacia, ISisfarmaService fisiotes, int listaDeArticulo) 
            : base(farmacia, fisiotes)
        {
            _listaDeArticulo = listaDeArticulo;
        }

        public override void LoadConfiguration()
        {            
            _perteneceFarmazul = bool.Parse(ConfiguracionPredefinida[Configuracion.FIELD_ES_FARMAZUL]);
            _puntosDeSisfarma = ConfiguracionPredefinida[Configuracion.FIELD_PUNTOS_SISFARMA];
            _cargarPuntos = ConfiguracionPredefinida[Configuracion.FIELD_CARGAR_PUNTOS];
            _fechaDePuntos = ConfiguracionPredefinida[Configuracion.FIELD_FECHA_PUNTOS];
            _soloPuntosConTarjeta = ConfiguracionPredefinida[Configuracion.FIELD_SOLO_PUNTOS_CON_TARJETA];
            _canjeoPuntos = ConfiguracionPredefinida[Configuracion.FIELD_CANJEO_PUNTOS];
        }

        public override void Process() => throw new NotImplementedException();        
    }
}