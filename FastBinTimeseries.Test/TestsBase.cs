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
        private readonly Stopwatch _stopwatch = new Stopwatch();

        #region Mode enum

        public enum Mode
        {
            OneTime,
            Create,
            Verify
        }

        #endregion

        private const string StoreDir = "Stored1";

        private readonly Dictionary<string, int> _files = new Dictionary<string, int>();

        public static Mode RunMode
        {
            get { return Mode.Create; }
        }

        public string GetBinFileName()
        {
            var stackTrace = new StackTrace();
            var frameInd = 1;
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
            return MakeFilename(filename, count);
        }

        [SetUp, TearDown]
        public void Cleanup()
        {
            if (_stopwatch.IsRunning)
            {
                Console.WriteLine("{0}: Total test time {1}", GetType().Name, _stopwatch.Elapsed);
                _stopwatch.Reset();
            }
            else
                _stopwatch.Start();

            if (RunMode == Mode.OneTime)
            {
                foreach (var i in _files)
                    for (int j = 1; j <= i.Value; j++)
                    {
                        string s = MakeFilename(i.Key, j);
                        if (File.Exists(s))
                            File.Delete(s);
                    }
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
            return string.Format("{0}{1}{2}.bsd", dir, filename, count);
        }
    }
}