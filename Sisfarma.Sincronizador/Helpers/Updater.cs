using System;
using System.Deployment.Application;
using System.Threading;
using System.Windows.Forms;

namespace Sisfarma.Sincronizador.Helpers
{
    public static class Updater
    {
        private static CancellationTokenSource _tokenSource;

        public static CancellationTokenSource GetTokenSource()
        {
            if (_tokenSource == null)
                _tokenSource = new CancellationTokenSource();

            return _tokenSource;
        }

        public static CancellationToken GetCancellationToken()
        {
            if (_tokenSource == null)            
                _tokenSource = new CancellationTokenSource();

            return _tokenSource.Token;
        }

        public static bool InstallUpdateSyncWithInfo()
        {
            if (!ApplicationDeployment.IsNetworkDeployed)
                return false;

            var AD = ApplicationDeployment.CurrentDeployment;
            try
            {
                if (!AD.CheckForDetailedUpdate().UpdateAvailable)
                    return false;

                AD.Update();
                Application.ExitThread();
                Application.Restart();                
                Application.Exit();
                return true;
            }
            catch (DeploymentDownloadException) { return false; }
            catch (InvalidDeploymentException) { return false; }
            catch (InvalidOperationException) { return false; }
        }

        public static bool UpdateHot()
        {
            if (!ApplicationDeployment.IsNetworkDeployed)
                return false;

            var AD = ApplicationDeployment.CurrentDeployment;
            try
            {
                if (!AD.CheckForDetailedUpdate().UpdateAvailable)
                    return false;
                
                AD.Update();                
                return true;                                
            }
            catch (DeploymentDownloadException) { return false; }
            catch (InvalidDeploymentException) { return false; }
            catch (InvalidOperationException) { return false; }
        }

        public static bool CheckUpdateSyncWithInfo()
        {
            if (!ApplicationDeployment.IsNetworkDeployed)
                return false;

            var AD = ApplicationDeployment.CurrentDeployment;
            try
            {
                if (!AD.CheckForDetailedUpdate().UpdateAvailable)
                    return false;

                return true;                
            }
            catch (DeploymentDownloadException) { return false; }
            catch (InvalidDeploymentException) { return false; }
            catch (InvalidOperationException) { return false; }
        }
    }
}
