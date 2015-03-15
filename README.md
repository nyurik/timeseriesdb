## Introduction ##

TimeSeriesDb is a .NET library designed to store vast amount of data series in flat files.  Items in sequence have a non-decreasing index, and may be accessed as a stream going either forward or in reverse. Storage is done as blocks with memory mapped files or streams, and dynamically generated serialization code. Optionally the size of data may be shrunk by storing deltas rather than full values using 7bit number encoding.

Requirements: Visual Studio 2010 or later (Might be backported if there is enough interest)

This is an open source project. If you want it improved, participate and let your suggestions be heard.

## Samples ##

The easiest way to try this library is to get it through [NuGet](http://nuget.org/packages/NYurik.TimeSeriesDb.Sample):

  * For Visual Studio 10, install [NuGet package manager](http://nuget.org/). VS 11 already has it built-in.
  * Create Console Application
  * Right click the project in Solution Explorer / Manage NuGet Packages
  * Select "Online", search for "timeseriesdb", and install [TimeSeriesDb Sample](http://nuget.org/packages/NYurik.TimeSeriesDb.Sample)
  * In your Program.cs:
    * add **`using NYurik.TimeSeriesDb.Samples;`** namespace
    * add **`Demo.Run();`** to the **`Main()`** method.
  * Run

## Downloading ##

From NuGet:
  * [Library only](http://nuget.org/packages/NYurik.TimeSeriesDb)
  * [Library + Samples](http://nuget.org/packages/NYurik.TimeSeriesDb.Sample) (see above)
From Source:
  * Checkout the source, build, run Sample project.

## Usage: BinSeriesFile and BinCompressedFile ##
BinSeriesFile and BinCompressedFile are two recommended methods to store time series. The first uses a direct memory copy to file, whereas BinCompressedFile creates dynamic code to do a field by field serialization using deltas to store 7-bit encoded numbers, which allow for significant compression compared to BinTimeseriesFile class. In 7-bit encoding, the first bit of each byte specifies if this will be the last byte representing the number.

```
// Create a new file for MyStruct data. Can also use BinSeriesFile for non-compressed storage
using (var file = new BinCompressedFile<UtcDateTime, MyStruct>("data.bts"))
{
   file.UniqueIndexes = true; // enforces index uniqueness
   file.InitializeNewFile(); // create file and write header
   file.AppendData(data); // append data (stream of ArraySegment<>)
}

// Read needed data. Data can be appended if opened with "true"
using (var file = (IEnumerableFeed<UtcDateTime, MyStruct>) BinaryFile.Open("data.bts", false))
{
    // Enumerate one item at a time maximum 10 items starting at fromIndex
    // (can also read one batch at a time with StreamSegments)
    foreach (var val in file.Stream(fromIndex, maxItemCount = 10)
        Console.WriteLine(val);
}
```
