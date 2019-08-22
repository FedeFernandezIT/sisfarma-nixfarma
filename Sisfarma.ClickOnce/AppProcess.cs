using System;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Windows.Forms;

namespace Sisfarma.ClickOnce
{
    public static class AppProcessHelper
    {
        private static Mutex instanceMutex;
        public static bool SetSingleInstance()
        {
            bool createdNew;
            instanceMutex = new Mutex(true, @"Local\" + Process.GetCurrentProcess().MainModule.ModuleName, out createdNew);
            return createdNew;
        }

        public static bool ReleaseSingleInstance()
        {
            if (instanceMutex == null) return false;

            instanceMutex.Close();
            instanceMutex = null;

            return true;
        }

        private static bool isRestartDisabled;
        private static bool canRestart;

        public static void BeginReStart()
        {
            // Note that we can restart
            canRestart = true;

            // Start the shutdown process
            Application.Exit();
        }

        public static void PreventRestart(bool state = true)
        {
            isRestartDisabled = state;
            if (state) canRestart = false;
        }

        public static void RestartIfRequired(int exitCode = 0)
        {
            // make sure to release the instance
            ReleaseSingleInstance();

            if (canRestart)
                //app is restarting...
                Application.Restart();
            else
                // app is stopping...
                Environment.Exit(exitCode);
        }

        public static string Version()
        {
            return Assembly.GetEntryAssembly().GetName().Version.ToString();
        }
    }
}
