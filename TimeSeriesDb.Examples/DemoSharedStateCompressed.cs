using System;
using System.Collections.Generic;
using System.IO;
using NYurik.TimeSeriesDb.Serializers.BlockSerializer;

// We are storing identical data to two files, disable warning
// ReSharper disable PossibleMultipleEnumeration

namespace NYurik.TimeSeriesDb.Examples
{
    /// <summary>
    /// This example demonstrates how to configure a compressed file to share common delta state between two fields.
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
    internal class DemoSharedStateCompressed : IExample
    {
        #region IExample Members

        public void Run()
        {
            string filename1 = GetType().Name + "1.bts";
            if (File.Exists(filename1)) File.Delete(filename1);
            string filename2 = GetType().Name + "2.bts";
            if (File.Exists(filename2)) File.Delete(filename2);

            // Create new BinCompressedSeriesFile file that stores a sequence of ItemLngDblDbl structs
            // The file is indexed by a long value inside ItemLngDblDbl marked with the [Index] attribute.
            using (var bf1 = new BinCompressedSeriesFile<long, ItemLngDblDbl>(filename1))
            using (var bf2 = new BinCompressedSeriesFile<long, ItemLngDblDbl>(filename2))
            {
                //
                // Configure value storage. This is the only difference with using BinSeriesFile.
                //
                // When a new instance of BinCompressedSeriesFile is created,
                // RootField will be pre-populated with default configuration objects.
                // Some fields, such as doubles, require additional configuration before the file can be initialized.
                //
                var root = (ComplexField) bf1.FieldSerializer.RootField;

                var fld1 = (ScaledDeltaField) root["Value1"].Field;
                var fld2 = (ScaledDeltaField) root["Value2"].Field;

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
                    Utils.GenerateData(1, 10000, i => new ItemLngDblDbl(i, i*10, i*10 + 1/(1.0 + i%100)));

                //
                // Append data to the file
                //

                bf1.AppendData(data);
                
                
                
                // Initialize the second file in an identical fashion without linking the states
                var root2 = (ComplexField) bf2.FieldSerializer.RootField;
                ((ScaledDeltaField) root2["Value1"].Field).Multiplier = 100;
                ((ScaledDeltaField) root2["Value2"].Field).Multiplier = 100;
                bf2.InitializeNewFile();

                // Append the same data to the second file
                bf2.AppendData(data);

                //
                // Read all data and print it using Stream() - one value at a time
                // This method is slower than StreamSegments(), but easier to use for simple one-value iteration
                //
                Console.WriteLine(
                    "Shared state:    FirstIndex = {0}, LastIndex = {1}, Size = {2} bytes",
                    bf1.FirstIndex, bf1.LastIndex, bf1.BaseStream.Length);

                Console.WriteLine(
                    "NonShared state: FirstIndex = {0}, LastIndex = {1}, Size = {2} bytes",
                    bf2.FirstIndex, bf2.LastIndex, bf2.BaseStream.Length);
            }

            // cleanup
            File.Delete(filename1);
            File.Delete(filename2);
        }

        #endregion
    }
}