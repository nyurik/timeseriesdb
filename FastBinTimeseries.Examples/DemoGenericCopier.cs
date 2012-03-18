#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 *     This file is part of FastBinTimeseries library
 * 
 *  FastBinTimeseries is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  FastBinTimeseries is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with FastBinTimeseries.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#endregion

using System;
using System.IO;
using NYurik.FastBinTimeseries.Serializers.BlockSerializer;

namespace NYurik.FastBinTimeseries.Examples
{
    internal static class DemoGenericCopier
    {
        public static void Run()
        {
            const string srcFile = "GenericCopier1.bts";
            const string dstFile = "GenericCopier2.bts";

            Console.WriteLine("\n **** GenericCopier example ****\n");

            try
            {
                // Create sample file and put some data into it
                CreateSampleFile(srcFile);

                // Open sample file in a generic way without specifying the item and index types
                using (var bf = BinaryFile.Open(srcFile, false))
                {
                    var src = bf as IEnumerableFeed;
                    if (src == null)
                    {
                        // This could be a BinIndexedFile or some legacy file that we no longer support.
                        // Even though BinaryFile supports RunGenericMethod() with one generic argument,
                        // for the purposes of this demo we will only show using it with two that IEnumerableFeed has.
                        Console.WriteLine("File {0} does not support reading through IEnumerableFeed<>", srcFile);
                        return;
                    }

                    // Print content of the source file
                    Console.WriteLine("Source file\n{0}", Utils.DumpFeed(src));

                    // We need a class that implements IGenericCallable2<,>
                    // As an alternative, "this" class could implement it
                    var callable = new GenericCopier();

                    // Run generic method on the callable class, passing string as a parameter to it
                    long copied = src.RunGenericMethod(callable, dstFile);

                    // Done
                    Console.WriteLine("{0} items was copied from {1} to {2}", copied, srcFile, dstFile);
                }
            }
            finally // Cleanup
            {
                if (File.Exists(srcFile)) File.Delete(srcFile);
                if (File.Exists(dstFile)) File.Delete(dstFile);
            }
        }

        private static void CreateSampleFile(string filename)
        {
            if (File.Exists(filename))
                File.Delete(filename);

            // Create and populate sample file
            // See DemoBinCompressedSeriesFile for more info
            using (var bf = new BinCompressedSeriesFile<long, ItemLngDbl>(filename))
            {
                var root = (ComplexField) bf.FieldSerializer.RootField;
                ((ScaledDeltaField) root["Value"].Field).Multiplier = 100;
                bf.InitializeNewFile();
                bf.AppendData(Utils.GenerateData(3, 10, i => new ItemLngDbl(i, i/100.0)));
            }
        }

        #region Nested type: GenericCopier

        private class GenericCopier : IGenericCallable2<long, string>
        {
            #region IGenericCallable2<long,string> Members

            /// <summary>
            ///   This method will be called with TInd and TVal properly set to what they are in a file
            /// </summary>
            public long Run<TInd, TVal>(IGenericInvoker source, string destinationFile)
                where TInd : IComparable<TInd>
            {
                // Any object that implements IEnumerableFeed will also implement IEnumerableFeed<,>
                // The source is the binary file object on which RunGenericMethod() was called
                var src = (IEnumerableFeed<TInd, TVal>) source;

                // Create BinSeriesFile as it is easier to set up than a compressed one
                using (var dst = new BinSeriesFile<TInd, TVal>(destinationFile))
                {
                    // Initialize new file
                    dst.InitializeNewFile();

                    // Copy the entire content of the source file into the destination file
                    dst.AppendData(src.StreamSegments());

                    // Dump content of the new file to the console
                    Console.WriteLine("Destination file\n{0}", Utils.DumpFeed(dst));

                    // Return item count (not supported for compressed files)
                    return dst.Count;
                }
            }

            #endregion
        }

        #endregion
    }
}