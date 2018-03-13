using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;

namespace Adaptive.Aeron.Driver
{
    public class Driver : IDisposable
    {
        internal const int CTRL_C_EVENT = 0;

        [DllImport("kernel32.dll")]
        internal static extern bool GenerateConsoleCtrlEvent(uint dwCtrlEvent, uint dwProcessGroupId);

        [DllImport("Kernel32", SetLastError = true)]
        private static extern bool SetConsoleCtrlHandler(HandlerRoutine handler, bool add);

        // Enumerated type for the control messages sent to the handler routine
        enum CtrlTypes
        {
            CTRL_C_EVENT = 0,
            CTRL_BREAK_EVENT,
            CTRL_CLOSE_EVENT,
            CTRL_LOGOFF_EVENT = 5,
            CTRL_SHUTDOWN_EVENT
        }
        
        // A delegate type to be used as the handler routine 
        // for SetConsoleCtrlHandler.
        private delegate bool HandlerRoutine(CtrlTypes CtrlType);
        
        // Delegate type to be used as the Handler Routine for SCCH
        delegate Boolean ConsoleCtrlDelegate(uint CtrlType);

        private Process _process;
        private readonly Dictionary<string, string> _options = new Dictionary<string, string>();

        public bool DeleteDriverOnStart
        {
            set => _options.Add("aeron.dir.delete.on.start", value.ToString());
        }

        public bool RunMediaDriver
        {
            set => _options.Add("aeron.start.archiver", value.ToString());
        }

        public bool RunArchiver
        {
            set => _options.Add("aeron.start.archiver", value.ToString());
        }

        public bool RunConsensusModule
        {
            set => _options.Add("aeron.start.archiver", value.ToString());
        }

        public string JavaExe { get; set; } = "java";

        public string ClassPath { get; set; } = "media-driver.jar";

        public string EntryPoint { get; set; } = "io.aeron.cluster.ClusteredMediaDriver";

        public IDictionary<string, string> Options => _options;

        public static Driver Launch()
        {
            var d = new Driver
            {
                DeleteDriverOnStart = true
            };
            d.Start();
            return d;
        }

        private static bool ParentConsoleCtrlCheck(CtrlTypes sig)
        {
            Console.WriteLine("parent: Received shutdown event");
            // Returning true prevents the default handler from running
            // which kills the parent
            return true;
        }
        
        public static void Main(string[] args)
        {
            SetConsoleCtrlHandler(ParentConsoleCtrlCheck, true);
            
            using (var driver = Driver.Launch())
            {
                Console.WriteLine("Press any key...");
                Console.ReadLine();
            }
            Console.WriteLine("Done...");
        }

        public void Start()
        {
            if (!Utitlity.CheckJava(JavaExe))
            {
                throw new FileNotFoundException("Cannot find java.");
            }

            var options = string.Join(" ", _options.Select(kvp => $"-D{kvp.Key}={kvp.Value}"));

            var p = new Process
            {
                StartInfo =
                {
                    FileName = JavaExe,
                    Arguments = $"-cp \"{ClassPath}\" {options} {EntryPoint}",
                    RedirectStandardInput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false
                }
            };

            p.Start();

            var s = Stopwatch.StartNew();

            while (s.ElapsedMilliseconds < 750)
            {
                if (p.HasExited)
                {
                    var error = p.StandardError.ReadToEnd();

                    Console.WriteLine(s.ElapsedMilliseconds);

                    if (error.Contains("ActiveDriverException"))
                    {
                        throw new InvalidOperationException("Aeron MediaDriver is already running.");
                    }


                    throw new Exception("Problem starting driver " + error);
                }

                Thread.Sleep(10);
            }


            _process = p;
        }

        public void Dispose()
        {
            var p = _process;
            _process = null;

            if (p == null)
            {
                return;
            }

            try
            {
                GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0);
                p.Close();
                p.WaitForExit();
                p.Dispose();
            }
            catch (Exception)
            {
                // ignored
            }
        }
    }
}