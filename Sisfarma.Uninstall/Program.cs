using Sisfarma.ClickOnce;
using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Windows.Forms;

namespace Sisfarma.Uninstall
{
    public class Program
    {
        private static Mutex instanceMutex;

        [STAThread]
        static void Main(string[] args)
        {
            try
            {
                bool createdNew;
                instanceMutex = new Mutex(true, @"Local\" + Assembly.GetExecutingAssembly().GetType().GUID, out createdNew);
                if (!createdNew)
                {
                    instanceMutex = null;
                    return;
                }
                if (DialogResult.Yes == MessageBox.Show($"¿Desea desinstalar {Globals.PublisherName} - {Globals.ProductName}", "Desinstalar", MessageBoxButtons.YesNo, MessageBoxIcon.Question))
                {
                    var clickOnce = new ClickOnceHelper(Globals.PublisherName, Globals.ProductName);
                    if (clickOnce.Uninstall())
                    {
                        clickOnce.RemoveStartup();
                        var publisherFolder = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), Globals.PublisherName);
                        if (Directory.Exists(publisherFolder))
                            Directory.Delete(publisherFolder, true);
                    }
                }
                ReleaseMutex();
            }
            catch (Exception e)
            {
                Debug.WriteLine(e.Message);
            }
        }

        private static void ReleaseMutex()
        {
            if (instanceMutex == null)
                return;
            instanceMutex.ReleaseMutex();
            instanceMutex.Close();
            instanceMutex = null;
        }
    }
}
