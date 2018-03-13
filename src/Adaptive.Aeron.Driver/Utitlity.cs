using System;
using System.Diagnostics;

namespace Adaptive.Aeron.Driver
{
    public static class Utitlity
    {
        public static bool CheckJava(string javaCommand)
        {
            try
            {
                var p = new Process
                {
                    StartInfo =
                    {
                        FileName = javaCommand,
                        Arguments = "-version"
                    }
                };

                p.Start();
                p.WaitForExit(1000);
                return p.ExitCode == 0;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}