using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Core.Extensions;
using Sisfarma.Sincronizador.Core.Helpers;
using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Sisfarma.Sincronizador.Domain.Core.Sincronizadores.SuperTypes
{
    public abstract class BaseSincronizador : Sincronizador, ISincronizadorAsync
    {
        protected ISisfarmaService _sisfarma;

        public BaseSincronizador(ISisfarmaService sisfarma)
            => _sisfarma = sisfarma ?? throw new ArgumentNullException(nameof(sisfarma));

        public override async Task SincronizarAsync(CancellationToken cancellationToken = default(CancellationToken), int delayLoop = 200)
        {
            _cancellationToken = cancellationToken;
            LoadConfiguration();

            PreSincronizacion();

            while (true)
            {
                try
                {
                    _cancellationToken.ThrowIfCancellationRequested();

                    Process();
                }
                catch (OperationCanceledException ex)
                {
                    throw ex;
                }
                catch (RestClientException ex)
                {
                    var error = ex.ToLogErrorMessage();
                    LogError(error);
                }
                catch (Exception ex)
                {
                    var error = ex.ToLogErrorMessage();
                    MessageBox.Show(error);

                    LogError(error);
                }
                finally
                {
                    await Task.Delay(delayLoop);
                }
            }
        }

        public virtual void LoadConfiguration()
        {
        }

        public virtual void PreSincronizacion()
        {
        }

        private void LogError(string message)
        {
            try
            {
                var hash = Cryptographer.GenerateMd5Hash(message);

                var logsPrevios = _sisfarma.Configuraciones.GetByCampo(Configuracion.FIELD_LOG_ERRORS);
                if (logsPrevios.Contains(hash))
                    return;

                var log = $@"$log{{{hash}}}{Environment.NewLine}{DateTime.UtcNow.ToString("o")}{Environment.NewLine}{message}";
                var logs = $@"{logsPrevios}{Environment.NewLine}{log}";
                _sisfarma.Configuraciones.Update(Configuracion.FIELD_LOG_ERRORS, logs);
            }
            catch (Exception)
            {
                // nothing
                // El sincro se detiene si lanzamos una excepción en este punto.
            }
        }
    }
}