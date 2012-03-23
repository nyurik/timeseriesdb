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

    internal static class Demo
    {
        private static readonly List<Type> AllSamples = new List<Type>();

        static Demo()
        {
            Add<DemoSimple>();
            Add<DemoCompressed>();
            Add<DemoGenericCopier>();
            Add<DemoSharedStateCompressed>();
            Add<DemoIndexWithNoStateCompressed>();
        }

        private static void Add<T>()
            where T : ISample, new()
        {
            AllSamples.Add(typeof (T));
        }

        private static void Run(Type type)
        {
            Console.WriteLine("\n **** Running {0} sample ****\n", type.Name);
            ((ISample) Activator.CreateInstance(type)).Run();
        }

        /// <summary>
        /// Runs all samples when no parameters is given,
        /// or the specific one by the demo class name.
        /// The "Demo" prefix in the sample name is optional.
        /// </summary>
        public static void Run(string sample = null)
        {
            try
            {
                if (!string.IsNullOrEmpty(sample)
                    && !sample.StartsWith("Demo", StringComparison.InvariantCultureIgnoreCase))
                    sample = "Demo" + sample;

                var found = false;
                if (!string.IsNullOrEmpty(sample))
                {
                    foreach (var s in AllSamples)
                    {
                        if (string.Equals(s.Name, sample, StringComparison.InvariantCultureIgnoreCase))
                        {
                            Run(s);
                            found = true;
                            break;
                        }
                    }
                }

                if (!found)
                {
                    Console.WriteLine("Running all samples\n");
                    foreach (Type s in AllSamples)
                        Run(s);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
    }
}