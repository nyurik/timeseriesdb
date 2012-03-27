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
using System.IO;
using System.Linq;
using NYurik.TimeSeriesDb.Serializers.BlockSerializer;

// Do not disable these Resharper checks in your code. Demo purposes only.
// ReSharper disable InconsistentNaming

namespace NYurik.TimeSeriesDb.Samples
{
    /// <summary>
    /// This sample demonstrates how to configure a simple compressed file.
    /// 
    /// The compressed file stores a series of Int64 values in a 7bit encoding, where the highest bit is cleared
    /// if this byte is the last one in sequence, or set if there are more bytes. Thus, the smaller the number stored,
    /// the less space it takes.
    /// 
    /// By default, each field is set-up with a <see cref="BaseField"/>-derrived descriptor fields that generates
    /// code required to serialize and deserialize the field value from the compressed stream. The goal of each
    /// field is to work with the smallest possible numbers to reduce overall size.
    /// 
    /// For example, market tick data could be described with three values: (Index, double Price, long Size)
    /// The index (timestamp) is increasing by small increments (milliseconds), and the price tend change very little,
    /// from the last value. Also, we can specify that the price is measured in to at most two significant digits.
    /// 
    /// In other words, given a sequence of ticks 1...N, the stored values can be:
    /// 
    ///   Index1, Price1, Size1,
    ///   (Index2-Index1), (Price2-Price1), (Size2-Size2),
    ///   ...
    ///   (IndexN - Index[N-1]), (PriceN - Price[N-1]), (SizeN-SizeN)
    /// 
    /// Note that PriceN is actually ((long)(price * 100.0)
    /// 
    /// For this demo, we will create a file that stores an item with a long index and a doubles Value.
    /// We will assume that the Values have at most two significant digits after the decimal point.
    /// </summary>
    internal class Demo_2_Compressed : ISample
    {
        #region ISample Members

        public void Run()
        {
            string filename = GetType().Name + ".bts";
            if (File.Exists(filename)) File.Delete(filename);

            // Create new BinCompressedSeriesFile file that stores a sequence of ItemLngDbl structs
            // The file is indexed by a long value inside ItemLngDbl marked with the [Index] attribute.
            using (var bf = new BinCompressedSeriesFile<long, ItemLngDbl>(filename))
            {
                //
                // Initialize new file parameters and create it
                //
                bf.UniqueIndexes = true; // enforce index uniqueness
                bf.Tag = "Sample Data"; // optionally provide a tag to store in the file header

                //
                // Configure value storage. This is the only difference with using BinSeriesFile.
                //
                // When a new instance of BinCompressedSeriesFile is created,
                // RootField will be pre-populated with default configuration objects.
                // Some fields, such as doubles, require additional configuration before the file can be initialized.
                //
                var root = (ComplexField) bf.FieldSerializer.RootField;

                // This double will contain values with no more than 2 digits after the decimal points.
                // Before serializing, multiply the value by 100 to convert to long.
                ((ScaledDeltaFloatField) root["Value"].Field).Multiplier = 100;

                bf.InitializeNewFile(); // Finish new file initialization and create an empty file


                // 
                // Set up data generator to generate 10 items starting with index 3
                //
                IEnumerable<ArraySegment<ItemLngDbl>> data = Utils.GenerateData(3, 10, i => new ItemLngDbl(i, i/100.0));


                //
                // Append data to the file
                //
                bf.AppendData(data);


                //
                // Read all data and print it using Stream() - one value at a time
                // This method is slower than StreamSegments(), but easier to use for simple one-value iteration
                //
                Console.WriteLine(" ** Content of file {0} after the first append", filename);
                Console.WriteLine("FirstIndex = {0}, LastIndex = {1}", bf.FirstIndex, bf.LastIndex);
                foreach (ItemLngDbl val in bf.Stream())
                    Console.WriteLine(val);
            }

            // Re-open the file, allowing data modifications
            // IWritableFeed<,> interface is better as it will work with non-compressed files as well
            using (var bf = (IWritableFeed<long, ItemLngDbl>) BinaryFile.Open(filename, true))
            {
                // Append a few more items with different ItemLngDbl.Value to tell them appart
                IEnumerable<ArraySegment<ItemLngDbl>> data = Utils.GenerateData(10, 10, i => new ItemLngDbl(i, i/25.0));

                // New data indexes will overlap with existing, so allow truncating old data
                bf.AppendData(data, true);

                // Print values
                Console.WriteLine("\n ** Content of file {0} after the second append", filename);
                Console.WriteLine("FirstIndex = {0}, LastIndex = {1}", bf.FirstIndex, bf.LastIndex);
                foreach (ItemLngDbl val in bf.Stream())
                    Console.WriteLine(val);
            }

            // Re-open the file for reading only (file can be opened for reading in parallel, but only one write)
            // IEnumerableFeed<,> interface is better as it will work with non-compressed files as well
            using (var bf = (IEnumerableFeed<long, ItemLngDbl>) BinaryFile.Open(filename, true))
            {
                // Show first item with index >= 5
                Console.WriteLine(
                    "\nFirst item on or after index 5 is {0}\n",
                    bf.Stream(5, maxItemCount: 1).First());

                // Show last item with index < 7 (iterate backwards)
                Console.WriteLine(
                    "Last item before index 7 is {0}\n",
                    bf.Stream(7, inReverse: true, maxItemCount: 1).First());

                // Average of values for indexes >= 4 and < 8
                Console.WriteLine(
                    "Average of values for indexes >= 4 and < 8 is {0}\n",
                    bf.Stream(4, 8).Average(i => i.Value));

                // Sum of the first 3 values with index less than 18 and going backwards
                Console.WriteLine(
                    "Sum of the first 3 values with index less than 18 and going backwards is {0}\n",
                    bf.Stream(18, maxItemCount: 3, inReverse: true).Sum(i => i.Value));
            }

            // cleanup
            File.Delete(filename);
        }

        #endregion
    }
}