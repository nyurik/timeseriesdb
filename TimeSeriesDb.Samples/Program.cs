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
using System.Diagnostics;

namespace NYurik.TimeSeriesDb.Samples
{
    internal static class Program
    {
        private static void Main(string[] args)
        {
            Demo.Run(args.Length > 0 ? args[0] : null);
            return;

            Run<bool>();
            Run<object>();
            Run<string>();
        }
        private  static void Run<T>()
        {
            const int count = 10000000;

            Test1<T>();
            Test2<T>();

            var sw1 = new Stopwatch();
            var sw2 = new Stopwatch();

            var sw = Stopwatch.StartNew();

            for (int j = 0; j < 10; j++)
            {
                sw1.Start();
                for (int i = 0; i < count; i++)
                {
                    Test1<T>();
                }
                sw1.Stop();

                sw2.Start();
                for (int i = 0; i < count; i++)
                {
                    Test2<T>();
                }
                sw2.Stop();
            }


            Console.WriteLine("{0} vs {1} (total={2})",sw1.Elapsed, sw2.Elapsed, sw.Elapsed);

        }


        static bool Test1<T>()
        {
            return typeof (T).IsValueType;
        }
        static bool Test2<T>()
        {
            return default(T) != null;
        }
    }
}