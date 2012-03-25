#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 *     This file is part of TimeSeriesDb library
 * 
 *  TimeSeriesDb is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  TimeSeriesDb is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with TimeSeriesDb.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#endregion

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using JetBrains.Annotations;
using NUnit.Framework;
using NYurik.TimeSeriesDb.Common;

namespace NYurik.TimeSeriesDb.Test
{
    public class TestsBase
    {
        private const Mode InitRunMode = Mode.OneTime;
        private const string StoreDir = "StoredOldNs2";
        private const string TestFileSuffix = ".testbsd";

        private const string RenamedNamespace = "NYurik.FastBinTimeseries.Test";
        private static readonly Assembly TestAssembly = typeof (TestsBase).Assembly;
        private static readonly Dictionary<string, Type> RenamedNsTypes;

        public static readonly Func<string, Type> LegacyResolver =
            TypeUtils.CreateCachingResolver(
                TypeResolver, LegacySupport.TypeResolver, TypeUtils.ResolverFromAnyAssemblyVersion);

        static TestsBase()
        {
            var newNs = typeof (TestsBase).Namespace + ".";

            // ReSharper disable PossibleNullReferenceException
            RenamedNsTypes =
                (from type in TestAssembly.GetTypes()
                 where type.IsPublic && !type.IsAbstract
                 select type).ToDictionary(i => i.FullName.Replace(newNs, RenamedNamespace + "."));
            // ReSharper restore PossibleNullReferenceException
        }

        protected static Type TypeResolver(TypeSpec spec, AssemblyName assemblyName)
        {
            if (assemblyName != null)
            {
                Type type;
                if (assemblyName.Name == RenamedNamespace && RenamedNsTypes.TryGetValue(spec.Name, out type))
                    return type;
            }

            return null;
        }

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
            Assert.IsNotNull(method.DeclaringType, "method.DeclaringType != null");
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
                TimeSpan elapsed = _stopwatch.Elapsed;
                Console.WriteLine("{0}: Total test time {1}", GetType().Name, elapsed);
                _stopwatch.Reset();
            }
            else
                _stopwatch.Start();

            DeleteTempFiles();
        }

        protected static void DeleteTempFiles()
        {
            if (RunMode == Mode.OneTime)
            {
//                GC.Collect();
//                GC.WaitForFullGCComplete();
                //GC.WaitForFullGCComplete(300);
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
            return String.Format("{0}{1}{2}{3}", dir, filename, count, TestFileSuffix);
        }

        /// <summary>
        ///   This method is useful during debugging to view IEnumerable's content as a string in the immediate window or expression eval
        /// </summary>
        [UsedImplicitly]
        public static string Dump<T>(IEnumerable<ArraySegment<T>> source)
        {
            return Dump(source.Stream());
        }

        /// <summary>
        ///   This method is useful during debugging to view IEnumerable's content as a string in the immediate window or expression eval
        /// </summary>
        public static string Dump<T>(IEnumerable<T> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            // ReSharper disable CompareNonConstrainedGenericWithNull
            Func<T, string> converter = i => i == null ? "" : i.ToString();
            // ReSharper restore CompareNonConstrainedGenericWithNull

            bool isFirst = true;
            var sb = new StringBuilder();
            foreach (T t in source)
            {
                if (isFirst)
                    isFirst = false;
                else
                    sb.Append(Environment.NewLine);

                sb.Append(converter(t));
            }
            return sb.ToString();
        }

        /// <summary>
        ///   This method is useful during debugging to view file's content as a string in the immediate window or expression eval
        /// </summary>
        [UsedImplicitly]
        public static string Dump(IEnumerableFeed f)
        {
            return f.RunGenericMethod(new DumpHelper(), null);
        }

        protected static IEnumerable<ArraySegment<T>> Data<T>(int minValue, int maxValue, int step = 1)
        {
            if (maxValue < minValue) throw new ArgumentException("max > min");
            if (step < 1) throw new ArgumentException("step < 1");
            if ((maxValue - minValue)%step != 0) throw new ArgumentException("max does not fall in step");

            var newObj = TestUtils.GetObjInfo<T>().Item1;
            var arr = new T[(maxValue - minValue)/step + 1];

            for (int ind = 0, val = minValue; val <= maxValue; val += step)
                arr[ind++] = newObj(val);

            return new[] {new ArraySegment<T>(arr)};
        }

        protected static IEnumerable<T> Join<T>(params IEnumerable<T>[] enmrs)
        {
            return enmrs.SelectMany(i => i);
        }

        #region Debug dump suppport

        private class DumpHelper : IGenericCallable2<string, object>
        {
            #region IGenericCallable2<string,object> Members

            public string Run<TInd, TVal>(IGenericInvoker2 source, object arg)
                where TInd : IComparable<TInd>
            {
                var f = (IEnumerableFeed<TInd, TVal>) source;

                var sb = new StringBuilder();
                foreach (TVal v in f.Stream())
                {
                    sb.Append(v);
                    sb.Append("\n");
                }
                return sb.Length == 0 ? "(empty)" : sb.ToString();
            }

            #endregion
        }

        #endregion

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