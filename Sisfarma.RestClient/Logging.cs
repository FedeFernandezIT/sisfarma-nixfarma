using System.IO;
using System.Threading;

namespace Sisfarma.RestClient
{
    public static class Logging
    {
        private static ReaderWriterLockSlim locker = new ReaderWriterLockSlim();

        public static void WriteToFileThreadSafe(string text, string path)
        {
            // Set Status to Locked
            locker.EnterWriteLock();
            try
            {
                var dir = Path.GetDirectoryName(path);
                if (!Directory.Exists(dir))
                    Directory.CreateDirectory(dir);

                // Append text to the file
                using (StreamWriter sw = File.AppendText(path))
                {
                    sw.WriteLine(text);
                    sw.Close();
                }
            }
            finally
            {
                // Release lock
                locker.ExitWriteLock();
            }
        }
    }
}