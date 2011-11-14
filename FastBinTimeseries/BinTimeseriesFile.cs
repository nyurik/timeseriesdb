using System;
using System.IO;
using System.Reflection;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// Helper non-generic class aids in creating a new instance of <see cref="BinTimeseriesFile{T}"/>.
    /// </summary>
    public static class BinTimeseriesFile
    {
        /// <summary>
        /// Uses reflection to create an instance of <see cref="BinTimeseriesFile{T}"/>.
        /// </summary>
        public static IBinTimeseriesFile GenericNew(Type itemType, string fileName, FieldInfo dateTimeFieldInfo = null)
        {
            return (IBinTimeseriesFile)
                   Activator.CreateInstance(
                       typeof (BinTimeseriesFile<>).MakeGenericType(itemType),
                       fileName, dateTimeFieldInfo);
        }
    }

    /// <summary>
    /// Object representing a binary-serialized timeseries file.
    /// This is a dummy wrapper to allow backward compatibility with the UtcDateTime index.
    /// </summary>
    public class BinTimeseriesFile<T> : BinSeriesFile<UtcDateTime, T>, IBinaryFile<T>, IBinTimeseriesFile,
                                        IEnumerableFeed<T>
    {
        /// <summary>
        /// Allow Activator non-public instantiation
        /// </summary>
        protected BinTimeseriesFile()
        {
        }

        /// <summary>
        /// Create new timeseries file. If the file already exists, an <see cref="IOException"/> is thrown.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the file to create.</param>
        /// <param name="indexFieldInfo">Field containing the UtcDateTime timestamp, or null to get default</param>
        public BinTimeseriesFile(string fileName, FieldInfo indexFieldInfo = null)
            : base(fileName, indexFieldInfo)
        {
        }

        #region IBinTimeseriesFile Members

        public FieldInfo TimestampFieldInfo
        {
            get { return IndexFieldInfo; }
            set { IndexFieldInfo = value; }
        }

        public UtcDateTime? FirstFileTS
        {
            get { return FirstFileIndex; }
        }


        public UtcDateTime? LastFileTS
        {
            get { return LastFileIndex; }
        }

        public bool UniqueTimestamps
        {
            get { return UniqueIndexes; }
            set { UniqueIndexes = value; }
        }

        #endregion

        #region IEnumerableFeed<T> Members

        public Func<T, UtcDateTime> TimestampAccessor
        {
            get { return IndexAccessor; }
        }

        #endregion
    }
}