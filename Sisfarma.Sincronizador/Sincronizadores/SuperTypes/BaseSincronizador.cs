using Sisfarma.RestClient.Exceptions;
using Sisfarma.Sincronizador.Extensions;
using Sisfarma.Sincronizador.Fisiotes;
using Sisfarma.Sincronizador.Helpers;
using System;
using System.Threading;
using System.Threading.Tasks;
using static Sisfarma.Sincronizador.Fisiotes.Repositories.ConfiguracionesRepository;

namespace Sisfarma.Sincronizador.Sincronizadores.SuperTypes
{
    public abstract class BaseSincronizador : Sincronizador, ISincronizadorAsync
    {
        private const string FIELD_LOG_ERRORS = FieldsConfiguracion.FIELD_LOG_ERRORS;

        protected FisiotesService _fisiotes;

        public BaseSincronizador(FisiotesService fisiotes)
            => _fisiotes = fisiotes ?? throw new ArgumentNullException(nameof(fisiotes));

        public override async Task SincronizarAsync(CancellationToken cancellationToken = default(CancellationToken), int delayLoop = 200)
        {
            Console.WriteLine($"{GetType().Name} init ...");
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
                    Console.WriteLine($"{GetType().Name} shutdown ...");
                    throw ex;
                }
                catch (RestClientException ex)
                {
                    LogError(ex.ToLogErrorMessage());
                }
                catch (Exception ex)
                {
                    LogError(ex.ToLogErrorMessage());
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

                var logsPrevios = _fisiotes.Configuraciones.GetByCampo(FIELD_LOG_ERRORS);
                if (logsPrevios.Contains(hash))
                    return;

                var log = $@"$log{{{hash}}}{Environment.NewLine}{DateTime.UtcNow.ToString("o")}{Environment.NewLine}{message}";
                var logs = $@"{logsPrevios}{Environment.NewLine}{log}";
                _fisiotes.Configuraciones.Update(FIELD_LOG_ERRORS, logs);
            }
            catch (Exception)
            {
                // nothing
                // El sincro se detiene si lanzamos una excepción en este punto.
            }
        }
    }
}