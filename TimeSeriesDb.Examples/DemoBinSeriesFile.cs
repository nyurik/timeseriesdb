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
using System.IO;
using System.Linq;

namespace NYurik.TimeSeriesDb.Examples
{
    internal static class DemoBinSeriesFile
    {
        public static void Run()
        {
            const string filename = "BinSeriesFile.bts";

            Console.WriteLine("\n **** BinSeriesFile<long, ItemLngDbl> example ****\n");

            if (File.Exists(filename))
                File.Delete(filename);

            // Create new BinSeriesFile file that stores a sequence of ItemLngDbl structs
            // The file is indexed by a long value inside ItemLngDbl marked with the [Index] attribute.
            using (var bf = new BinSeriesFile<long, ItemLngDbl>(filename))
            {
                //
                // Initialize new file parameters and create it
                //
                bf.UniqueIndexes = true; // enforce index uniqueness
                bf.Tag = "Sample Data"; // optionally provide a tag to store in the file header
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
            // IWritableFeed<,> interface is better as it will work with compressed files as well
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
            // IEnumerableFeed<,> interface is better as it will work with compressed files as well
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
    }
}