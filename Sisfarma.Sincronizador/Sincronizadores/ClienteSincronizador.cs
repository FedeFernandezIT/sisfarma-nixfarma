using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Farmatic;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Fisiotes.Models;
using Sisfarma.Sincronizador.Helpers;
using Sisfarma.Sincronizador.Sincronizadores.SuperTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Sisfarma.Sincronizador.Sincronizadores
{
    public class ClienteSincronizador : TaskSincronizador
    {
        private readonly string[] _horariosDeVaciamiento;

        private readonly bool _hasSexo;

        private string _puntosDeSisfarma;
        private bool _perteneceFarmazul;
        private bool _debeCargarPuntos;
        private int _ultimoClienteSincronizado;

        public ClienteSincronizador(FarmaticService farmatic, FisiotesService fisiotes)
            : base(farmatic, fisiotes)
        {
            _horariosDeVaciamiento = new[] { "1400", "2100"};            
            _hasSexo = farmatic.Clientes.HasSexoField();
        }

        public override void Process() => ProcessClientes();

        public override void LoadConfiguration()
        {
            _puntosDeSisfarma = ConfiguracionPredefinida[Configuracion.FIELD_PUNTOS_SISFARMA];
            //_perteneceFarmazul = _fisiotes.Configuraciones.PerteneceFarmazul();
            _perteneceFarmazul = bool.Parse(ConfiguracionPredefinida[Configuracion.FIELD_ES_FARMAZUL]);
            _debeCargarPuntos = _puntosDeSisfarma.ToLower().Equals("no") || string.IsNullOrWhiteSpace(_puntosDeSisfarma);
        }

        public override void PreSincronizacion()
        {
            _fisiotes.Clientes.ResetDniTracking();
            _ultimoClienteSincronizado = -1;
        }

        public void ProcessClientes()
        {
            if (_horariosDeVaciamiento.Any(x => x.Equals(DateTime.Now.ToString("HHmm"))))
            {
                _ultimoClienteSincronizado = -1;
            }

            var localClientes = _farmatic.Clientes.GetGreatThanId(_ultimoClienteSincronizado);

            var contadorHuecos = -1;
            foreach (var cliente in localClientes)
            {
                Task.Delay(5);

                _cancellationToken.ThrowIfCancellationRequested();

                if (contadorHuecos == -1)
                    contadorHuecos = Convert.ToInt32(cliente.IDCLIENTE);

                InsertOrUpdateCliente(cliente);

                var intIdCliente = Convert.ToInt32(cliente.IDCLIENTE);
                if (intIdCliente != contadorHuecos)
                {
                    var huecos = new List<string>();
                    for (int i = contadorHuecos; i < intIdCliente; i++)
                    {
                        huecos.Add(i.ToString());
                    }

                    if (huecos.Any())
                        _fisiotes.Huecos.Insert(huecos.ToArray());

                    contadorHuecos = intIdCliente;
                }
                contadorHuecos++;
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
                    beBlue);
                }
                else
                {
                    _fisiotes.Clientes.InsertOrUpdateBeBlue(
                        clienteDTO.Trabajador, clienteDTO.Tarjeta, cliente.IDCLIENTE, dniCliente, clienteDTO.Nombre.Strip(), clienteDTO.Telefono, clienteDTO.Direccion.Strip(),
                        clienteDTO.Movil, clienteDTO.Email, clienteDTO.FechaNacimiento, clienteDTO.Sexo, clienteDTO.FechaAlta, clienteDTO.Baja, clienteDTO.Lopd,
                        beBlue);
                }
            }
            else if (_debeCargarPuntos)
            {
                _fisiotes.Clientes.InsertOrUpdate(
                    clienteDTO.Trabajador, clienteDTO.Tarjeta, cliente.IDCLIENTE, dniCliente, clienteDTO.Nombre.Strip(), clienteDTO.Telefono, clienteDTO.Direccion.Strip(),
                    clienteDTO.Movil, clienteDTO.Email, clienteDTO.Puntos, clienteDTO.FechaNacimiento, clienteDTO.Sexo, clienteDTO.FechaAlta, clienteDTO.Baja, clienteDTO.Lopd);
            }
            else
            {
                _fisiotes.Clientes.InsertOrUpdate(
                    clienteDTO.Trabajador, clienteDTO.Tarjeta, cliente.IDCLIENTE, dniCliente, clienteDTO.Nombre.Strip(), clienteDTO.Telefono, clienteDTO.Direccion.Strip(),
                    clienteDTO.Movil, clienteDTO.Email, clienteDTO.FechaNacimiento, clienteDTO.Sexo, clienteDTO.FechaAlta, clienteDTO.Baja, clienteDTO.Lopd);
            }

            _ultimoClienteSincronizado = cliente.IDCLIENTE.ToIntegerOrDefault();
        }
    }
}