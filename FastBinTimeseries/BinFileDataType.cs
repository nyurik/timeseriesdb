using System;

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// Type of data to contain the file.
    /// ATTENTION!  All new enum values must be added at the end of the list.
    /// Never rearrange or delete any thems! Use [Obsolete] attribute if needed.
    /// </summary>
    public enum BinFileDataType
    {
        [TypeInfo(null)] Custom = 0x1234,
        [TypeInfo(typeof (float))] Floats,
        [TypeInfo(typeof (double))] Doubles,
        [TypeInfo(typeof (int))] Ints,
        [TypeInfo(typeof (long))] Longs,
        [TypeInfo(typeof (short))] Shorts,
        [TypeInfo(typeof (byte))] Bytes,
        [TypeInfo(typeof (char))] Chars,
        [TypeInfo(typeof (DateTime))] DateTimes,
        [TypeInfo(typeof (TimeSpan))] TimeSpans

        // ATTENTION!  All new enum values must be added at the end of the list.
        // Never rearrange or delete any thems! Use [Obsolete] attribute if needed.
    }
}