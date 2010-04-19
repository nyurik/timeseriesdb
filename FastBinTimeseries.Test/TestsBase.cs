using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    public class TestsBase
    {
        private const Mode InitRunMode = Mode.Create;
        private const string StoreDir = "Stored2";
        private const string TestFileSuffix = ".testbsd";

        private readonly Dictionary<string, int> _files = new Dictionary<string, int>();
        private readonly Stopwatch _stopwatch = new Stopwatch();

        protected static Mode RunMode
        {
            get { return InitRunMode; }
        }

        protected static bool AllowCreate
        {
            get { return RunMode != Mode.Verify; }
        }

        protected string GetBinFileName()
        {
            var stackTrace = new StackTrace();
            int frameInd = 1;
            MethodBase method;
            for (int i = frameInd; i < stackTrace.FrameCount; i++)
            {
                method = stackTrace.GetFrame(i).GetMethod();
                if (method.GetCustomAttributes(typeof (TestAttribute), true).Length > 0)
                {
                    frameInd = i;
                    break;
                }
            }

            method = stackTrace.GetFrame(frameInd).GetMethod();
            string filename = method.DeclaringType.Name + "." + method.Name + "_";
            filename = filename.Replace("<", "_").Replace(">", "_");

            int count;
            _files.TryGetValue(filename, out count);
            count++;
            _files[filename] = count;
            
            filename = MakeFilename(filename, count);
            if (AllowCreate && File.Exists(filename))
                File.Delete(filename);
            return filename;
        }

        [SetUp, TearDown]
        public void Cleanup()
        {
            if (_stopwatch.IsRunning)
            {
                Console.WriteLine("{0}: Total test time {1}", GetType().Name, _stopwatch.Elapsed);
                _stopwatch.Reset();
//                GC.Collect();
//                GC.WaitForFullGCComplete(300);
            }
            else
                _stopwatch.Start();

            DeleteTempFiles();
        }

        protected static void DeleteTempFiles()
        {
            if (RunMode == Mode.OneTime)
            {
                foreach (string file in Directory.GetFiles(".", "*" + TestFileSuffix, SearchOption.TopDirectoryOnly))
                    File.Delete(file);
            }
        }

        private static string MakeFilename(string filename, int count)
        {
            string dir = "";
            if (RunMode != Mode.OneTime)
            {
                if (!Directory.Exists(StoreDir))
                    Directory.CreateDirectory(StoreDir);
                dir = StoreDir + "\\";
            }
            return string.Format("{0}{1}{2}{3}", dir, filename, count, TestFileSuffix);
        }

        #region Nested type: Mode

        protected enum Mode
        {
            OneTime,
            Create,
            Verify
        }

        #endregion
    }
}