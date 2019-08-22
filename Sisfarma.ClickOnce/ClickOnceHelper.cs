using Microsoft.Win32;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Threading;

namespace Sisfarma.ClickOnce
{
    public class ClickOnceHelper
    {
        private const string UninstallString = "UninstallString";
        private const string DisplayNameKey = "DisplayName";
        private const string UninstallStringFile = "UninstallString.bat";
        private const string ApprefExtension = ".appref-ms";
        private readonly RegistryKey UninstallRegistryKey;

        private static string Location
        {
            get { return Assembly.GetExecutingAssembly().Location; }
        }

        public string PublisherName { get; private set; }
        public string ProductName { get; private set; }
        public string UninstallFile { get; private set; }

        public ClickOnceHelper(string publisher, string product)
        {
            PublisherName = publisher;
            ProductName = product;

            var publisherFolder = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), PublisherName);
            if (!Directory.Exists(publisherFolder))
                Directory.CreateDirectory(publisherFolder);
            UninstallFile = Path.Combine(publisherFolder, UninstallStringFile);
            UninstallRegistryKey = GetUninstallRegistryKeyByProductName(ProductName);
        }

        private RegistryKey GetUninstallRegistryKeyByProductName(string productName)
        {
            var subKey = Registry.CurrentUser.OpenSubKey(@"Software\Microsoft\Windows\CurrentVersion\Uninstall");
            if (subKey == null)
                return null;
            foreach (var name in subKey.GetSubKeyNames())
            {
                var application = subKey.OpenSubKey(name, RegistryKeyPermissionCheck.ReadWriteSubTree, RegistryRights.QueryValues | RegistryRights.ReadKey | RegistryRights.SetValue);
                if (application == null)
                    continue;
                foreach (var appKey in application.GetValueNames().Where(appKey => appKey.Equals(DisplayNameKey)))
                {
                    if (application.GetValue(appKey).Equals(productName))
                        return application;
                    break;
                }
            }
            return null;
        }

        public void UpdateUninstallParameters()
        {
            if (UninstallRegistryKey == null)
                return;
            var uninstallString = (string)UninstallRegistryKey.GetValue(UninstallString);
            if (!String.IsNullOrEmpty(UninstallFile) && uninstallString.StartsWith("rundll32.exe"))
            {
                File.WriteAllText(UninstallFile, uninstallString);
            }

            var str = String.Format("\"{0}\" uninstall", Path.Combine(Path.GetDirectoryName(Location), "uninstall.exe"));
            UninstallRegistryKey.SetValue(UninstallString, str);
        }

        public void RemoveStartup()
        {
            RegistryKey reg = Registry.CurrentUser.OpenSubKey("SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Run", true);
            foreach (var item in reg.GetValueNames())
            {
                if (item.Equals(ProductName))
                    reg.DeleteValue(ProductName);
            }
        }


        public bool Uninstall()
        {
            try
            {
                return ClickOnceRemovalDialog();
            }
            catch (Exception e)
            {
                Debug.WriteLine(e.Message);
                return false;
            }
        }

        [DllImport("user32.dll")]
        private static extern bool SetForegroundWindow(IntPtr hWnd);
        [DllImport("user32.dll", SetLastError = true)]
        static extern bool PostMessage(IntPtr hWnd, [MarshalAs(UnmanagedType.U4)] uint Msg, IntPtr wParam, IntPtr lParam);
        const int WM_KEYDOWN = 0x0100;

        private bool ClickOnceRemovalDialog()
        {
            try
            {
                if (!File.Exists(UninstallFile))
                    return false;

                var uninstallString = File.ReadAllText(UninstallFile);
                var fileName = uninstallString.Substring(0, uninstallString.IndexOf(" "));
                var args = uninstallString.Substring(uninstallString.IndexOf(" ") + 1);

                var proc = new Process
                {
                    StartInfo =
                {
                    Arguments = args,
                    FileName = fileName,
                    UseShellExecute = false
                }
                };
                proc.Start();
                RespondToClickOnceRemovalDialog();
                return true;
            }
            catch (Exception e)
            {
                Debug.WriteLine(e.Message);
                return false;
            }
        }

        private void RespondToClickOnceRemovalDialog()
        {
            var myWindowHandle = IntPtr.Zero;
            for (var i = 0; i < 250 && myWindowHandle == IntPtr.Zero; i++)
            {
                Thread.Sleep(150);
                foreach (var proc in Process.GetProcessesByName("dfsvc"))
                    if (!String.IsNullOrEmpty(proc.MainWindowTitle) && proc.MainWindowTitle.EndsWith(ProductName))
                    {
                        myWindowHandle = proc.MainWindowHandle;
                        break;
                    }
            }
            if (myWindowHandle == IntPtr.Zero)
                return;

            SetForegroundWindow(myWindowHandle);
            Thread.Sleep(100);
            const uint wparam = 0 << 29 | 0;

            PostMessage(myWindowHandle, WM_KEYDOWN, (IntPtr)Keys.Tab, (IntPtr)wparam);
            PostMessage(myWindowHandle, WM_KEYDOWN, (IntPtr)Keys.Down, (IntPtr)wparam);
            PostMessage(myWindowHandle, WM_KEYDOWN, (IntPtr)Keys.Tab, (IntPtr)wparam);
            PostMessage(myWindowHandle, WM_KEYDOWN, (IntPtr)Keys.Enter, (IntPtr)wparam);
        }
    }
}
