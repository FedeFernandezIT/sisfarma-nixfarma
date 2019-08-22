using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Helpers;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class HuecoSincronizador : TaskSincronizador
    {
        private readonly bool _hasSexo;

        private string _puntosDeSisfarma;
        private bool _perteneceFarmazul;
        private bool _debeCargarPuntos;

        public HuecoSincronizador(FarmaticService farmatic, FisiotesService fisiotes)
            : base(farmatic, fisiotes)
        {
            _hasSexo = farmatic.Clientes.HasSexoField();
        }

        public override void LoadConfiguration()
        {
            _puntosDeSisfarma = ConfiguracionPredefinida[Configuracion.FIELD_PUNTOS_SISFARMA];
            //_perteneceFarmazul = _fisiotes.Configuraciones.PerteneceFarmazul();
            _perteneceFarmazul = bool.Parse(ConfiguracionPredefinida[Configuracion.FIELD_ES_FARMAZUL]);
            _debeCargarPuntos = _puntosDeSisfarma.ToLower().Equals("no") || string.IsNullOrWhiteSpace(_puntosDeSisfarma);
        }

        public override void Process() => ProcessClientesHuecos();

        private void ProcessClientesHuecos()
        {
            var remoteHuecos = _fisiotes.Huecos.GetByOrderAsc();

            foreach (var hueco in remoteHuecos)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                var cliente = _farmatic.Clientes.GetOneOrDefaulById(hueco.ToIntegerOrDefault());
                if (cliente != null)
                    InsertOrUpdateCliente(cliente);
            }
        }

        private void InsertOrUpdateCliente(Farmatic.Models.Cliente cliente)
        {
            var clienteDTO = Generator.GenerarCliente(_farmatic, cliente, _hasSexo);

            var dniCliente = cliente.PER_NIF.Strip();

            if (_perteneceFarmazul)
            {
                var beBlue = _farmatic.Clientes.EsBeBlue(cliente.XTIPO_IDTIPO) ? 1 : 0;
                if (_debeCargarPuntos)
                {
                    _fisiotes.Clientes.InsertOrUpdateBeBlue(
                    clienteDTO.Trabajador, clienteDTO.Tarjeta, cliente.IDCLIENTE, dniCliente, clienteDTO.Nombre.Strip(), clienteDTO.Telefono, clienteDTO.Direccion.Strip(),
                    clienteDTO.Movil, clienteDTO.Email, clienteDTO.Puntos, clienteDTO.FechaNacimiento, clienteDTO.Sexo, clienteDTO.FechaAlta, clienteDTO.Baja, clienteDTO.Lopd,
                    beBlue, esHueco: true);
                }
                else
                {
                    _fisiotes.Clientes.InsertOrUpdateBeBlue(
                        clienteDTO.Trabajador, clienteDTO.Tarjeta, cliente.IDCLIENTE, dniCliente, clienteDTO.Nombre.Strip(), clienteDTO.Telefono, clienteDTO.Direccion.Strip(),
                        clienteDTO.Movil, clienteDTO.Email, clienteDTO.FechaNacimiento, clienteDTO.Sexo, clienteDTO.FechaAlta, clienteDTO.Baja, clienteDTO.Lopd,
                        beBlue, esHueco: true);
                }
            }
            else if (_debeCargarPuntos)
            {
                _fisiotes.Clientes.InsertOrUpdate(
                    clienteDTO.Trabajador, clienteDTO.Tarjeta, cliente.IDCLIENTE, dniCliente, clienteDTO.Nombre.Strip(), clienteDTO.Telefono, clienteDTO.Direccion.Strip(),
                    clienteDTO.Movil, clienteDTO.Email, clienteDTO.Puntos, clienteDTO.FechaNacimiento, clienteDTO.Sexo, clienteDTO.FechaAlta, clienteDTO.Baja, clienteDTO.Lopd,
                    esHueco: true);
            }
            else
            {
                _fisiotes.Clientes.InsertOrUpdate(
                    clienteDTO.Trabajador, clienteDTO.Tarjeta, cliente.IDCLIENTE, dniCliente, clienteDTO.Nombre.Strip(), clienteDTO.Telefono, clienteDTO.Direccion.Strip(),
                    clienteDTO.Movil, clienteDTO.Email, clienteDTO.FechaNacimiento, clienteDTO.Sexo, clienteDTO.FechaAlta, clienteDTO.Baja, clienteDTO.Lopd,
                    esHueco: true);
            }
        }
    }
}