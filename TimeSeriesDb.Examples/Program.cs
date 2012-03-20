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

namespace NYurik.TimeSeriesDb.Examples
{
    internal interface IExample
    {
        void Run();
    }

    internal static class Program
    {
        private static readonly Dictionary<string, Action> Examples
            = new Dictionary<string, Action>(StringComparer.InvariantCultureIgnoreCase);

        static Program()
        {
            AddExample<DemoSimple>();
            AddExample<DemoCompressed>();
            AddExample<DemoSharedStateCompressed>();
            AddExample<DemoGenericCopier>();
        }

        private static void AddExample<T>()
            where T : IExample, new()
        {
            Examples.Add(typeof (T).Name.Replace("Demo", ""), RunExample<T>);
        }

        private static void RunExample<T>()
            where T : IExample, new()
        {
            Console.WriteLine("\n **** Running {0} example ****\n", typeof (T).Name);
            new T().Run();
        }

        /// <summary>
        /// Runs all examples when no parameters is given,
        /// or the specific one provided by the first parameter
        /// </summary>
        private static void Main(string[] args)
        {
            try
            {
                Action run;
                if (args.Length > 0 && Examples.TryGetValue(args[0], out run))
                {
                    run();
                }
                else
                {
                    Console.WriteLine("Running all examples\n");
                    foreach (Action r in Examples.Values)
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