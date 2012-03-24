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
using System.IO;
using NYurik.TimeSeriesDb.Serializers.BlockSerializer;

namespace NYurik.TimeSeriesDb.Samples
{
    internal class DemoGenericCopier : ISample
    {
        #region ISample Members

        public void Run()
        {
            string srcFile = GetType().Name + "1.bts";
            if (File.Exists(srcFile)) File.Delete(srcFile);

            string dstFile = GetType().Name + "2.bts";
            if (File.Exists(dstFile)) File.Delete(dstFile);

            try
            {
                // Create sample file and put some data into it
                CreateSampleFile(srcFile);

                // Open sample file in a generic way without specifying the item and index types
                using (var bf = BinaryFile.Open(srcFile))
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

        #endregion

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
            public long Run<TInd, TVal>(IGenericInvoker2 source, string destinationFile)
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