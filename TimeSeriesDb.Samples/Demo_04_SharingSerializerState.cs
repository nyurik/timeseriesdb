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
// ReSharper disable PossibleMultipleEnumeration
// ReSharper disable InconsistentNaming

namespace NYurik.TimeSeriesDb.Samples
{
    /// <summary>
    /// This sample demonstrates how to configure a compressed file to share common delta state between two fields.
    /// 
    /// The compressed file stores a series of Int64 values in a 7bit encoding, where the highest bit is cleared
    /// if this byte is the last one in sequence, or set if there are more bytes. Thus, the smaller the number stored,
    /// the less space it takes.
    /// 
    /// By default, each field is set-up with its own state variable, so the delta is calculated between 
    /// each subsequent element's given field, but not within one element's different fields. Yet sometimes we need 
    /// to store items with related fields, and a shared state could result in a better compression.
    /// One such example can be (Index,Open,High,Low,Close) bars, where all four value fields can share the state,
    /// thus the delta would be the difference between last bar's close and next bar open, than with the high, etc.
    /// 
    /// In other words, given a sequence of bars 1...N, the stored values can be:
    /// 
    ///   Index1, Open1, (High1-Open1), (Low1-High1), (Close1-High1),
    ///   (Index2-Index1), (Open2-Close1), (High2-Open2), (Low2-High2), (Close2-High2),
    ///   ...
    ///   (IndexN - Index[N-1]), (OpenN - Close[N-1]), (HighN-OpenN), (LowN-HighN), (CloseN-HighN).
    /// 
    /// Note that there were two states here: one for the deltas between indexes, and one for all OHLC values.
    /// 
    /// For this demo, we will create a file that stores an item with a long index and two doubles Value1 and Value2.
    /// We will assume that the Values have at most two significant digits after the decimal point,
    /// and that the numbers tend to be somewhat correlated - like min and max temperature each hour.
    /// </summary>
    internal class Demo_04_SharingSerializerState : ISample
    {
        #region ISample Members

        public void Run()
        {
            string filename1 = GetType().Name + "1.bts";
            if (File.Exists(filename1)) File.Delete(filename1);
            string filename2 = GetType().Name + "2.bts";
            if (File.Exists(filename2)) File.Delete(filename2);
            string filename3 = GetType().Name + "3.bts";
            if (File.Exists(filename3)) File.Delete(filename3);

            // Create new BinCompressedSeriesFile file that stores a sequence of ItemLngDblDbl structs
            // The file is indexed by a long value inside ItemLngDblDbl marked with the [Index] attribute.
            // For comparison sake, also create identical but non-state-linked compressed and uncompressed.
            using (var bf1 = new BinCompressedSeriesFile<long, ItemLngDblDbl>(filename1))
            using (var bf2 = new BinCompressedSeriesFile<long, ItemLngDblDbl>(filename2))
            using (var bf3 = new BinSeriesFile<long, ItemLngDblDbl>(filename3))
            {
                //
                // Configure value storage. This is the only difference with using BinSeriesFile.
                //
                // When a new instance of BinCompressedSeriesFile is created,
                // RootField will be pre-populated with default configuration objects.
                // Some fields, such as doubles, require additional configuration before the file can be initialized.
                //
                var root = (ComplexField) bf1.RootField;

                var fld1 = (ScaledDeltaFloatField) root["Value1"].Field;
                var fld2 = (ScaledDeltaFloatField) root["Value2"].Field;

                // This double will contain values with no more than 2 digits after the decimal points.
                // Before serializing, multiply the value by 100 to convert to long.
                fld1.Multiplier = 100;
                fld2.Multiplier = 100;

                // ** IMPORTANT: Set the second field's state name the same as the first field, linking them together
                fld2.StateName = fld1.StateName;

                bf1.InitializeNewFile(); // Finish new file initialization and create an empty file


                // 
                // Set up data generator to generate items with closely related value1 and value2
                //
                IEnumerable<ArraySegment<ItemLngDblDbl>> data =
                    Utils.GenerateData(1, 10000, i => new ItemLngDblDbl(i, i*10, i*10 + Math.Round(1/(1.0 + i%100), 2)));

                //
                // Append data to the file
                //

                bf1.AppendData(data);


                //
                // Initialize the second in an identical fashion without linking the states and append the same data
                //
                var root2 = (ComplexField) bf2.RootField;
                ((ScaledDeltaFloatField) root2["Value1"].Field).Multiplier = 100;
                ((ScaledDeltaFloatField) root2["Value2"].Field).Multiplier = 100;
                bf2.InitializeNewFile();
                bf2.AppendData(data);

                //
                // Initialize the third uncompressed file and append the same data.
                //
                bf3.InitializeNewFile();
                bf3.AppendData(data);

                //
                // Print file sizes to see if there was any benefit
                //
                Console.WriteLine("      Shared: {0,10:#,#} bytes", bf1.BaseStream.Length);
                Console.WriteLine("   NonShared: {0,10:#,#} bytes", bf2.BaseStream.Length);
                Console.WriteLine("Uncompressed: {0,10:#,#} bytes", bf3.BaseStream.Length);
                Console.WriteLine();

                if (!bf1.Stream().SequenceEqual(bf2.Stream()))
                    throw new BinaryFileException("File #1 != #2");
                if (!bf1.Stream().SequenceEqual(bf3.Stream()))
                    throw new BinaryFileException("File #1 != #3");
            }

            //
            // Check that the settings are stored ok in the file and can be re-initialized on open
            //
            using (var bf1 = (IWritableFeed<long, ItemLngDblDbl>)BinaryFile.Open(filename1))
            using (var bf2 = (IWritableFeed<long, ItemLngDblDbl>)BinaryFile.Open(filename2))
            {
                if (!bf1.Stream().SequenceEqual(bf2.Stream()))
                    throw new BinaryFileException("File #1 != #2");
            }

            // cleanup
            File.Delete(filename1);
            File.Delete(filename2);
            File.Delete(filename3);
        }

        #endregion
    }
}