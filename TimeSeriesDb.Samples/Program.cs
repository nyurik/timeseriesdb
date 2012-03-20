#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

#endregion

using System;
using System.Collections.Generic;

namespace NYurik.TimeSeriesDb.Samples
{
    internal interface ISample
    {
        void Run();
    }

    internal static class Program
    {
        private static readonly Dictionary<string, Action> Samples
            = new Dictionary<string, Action>(StringComparer.InvariantCultureIgnoreCase);

        static Program()
        {
            Add<DemoSimple>();
            Add<DemoCompressed>();
            Add<DemoSharedStateCompressed>();
            Add<DemoGenericCopier>();
        }

        private static void Add<T>()
            where T : ISample, new()
        {
            Samples.Add(typeof (T).Name.Replace("Demo", ""), Run<T>);
        }

        private static void Run<T>()
            where T : ISample, new()
        {
            Console.WriteLine("\n **** Running {0} sample ****\n", typeof (T).Name);
            new T().Run();
        }

        /// <summary>
        /// Runs all samples when no parameters is given,
        /// or the specific one provided by the first parameter
        /// </summary>
        private static void Main(string[] args)
        {
            try
            {
                Action run;
                if (args.Length > 0 && Samples.TryGetValue(args[0], out run))
                {
                    run();
                }
                else
                {
                    Console.WriteLine("Running all samples\n");
                    foreach (Action r in Samples.Values)
                        r();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
    }
}