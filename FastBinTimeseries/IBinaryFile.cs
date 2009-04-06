using System;

namespace NYurik.FastBinTimeseries
{
    public interface IBinaryFile : IDisposable
    {
        /// <summary>
        /// Access to the non-generic instance of the current serializer
        /// </summary>
        IBinSerializer NonGenericSerializer { get; }

        /// <summary>Total number of items in the file</summary>
        long Count { get; }

        /// <summary>The size of each item of data in bytes</summary>
        int ItemSize { get; }

        /// <summary>User string stored in the header</summary>
        string Tag { get; set; }

        /// <summary>
        /// True when the file has no data
        /// </summary>
        bool IsEmpty { get; }

        /// <summary>
        /// True if the file is ready for read/write operations
        /// </summary>
        bool IsOpen { get; }

        /// <summary>
        /// Can be changed at any time. Enables MMF access mode.
        /// </summary>
        bool EnableMemoryMappedFileAccess { get; set; }

        /// <summary>
        /// Full path to the file
        /// </summary>
        string FileName { get; }

        /// <summary>
        /// Base version of the serializer that was used to create this file
        /// </summary>
        Version BaseVersion { get; }

        /// <summary>Size of the file header in bytes</summary>
        int HeaderSize { get; }

        /// <summary>Was file open for writing</summary>
        bool CanWrite { get; }

        /// <summary>
        /// The version of the serializer used to create this file
        /// </summary>
        Version SerializerVersion { get; }

        /// <summary>
        /// The version of the binary file handler used to create this file
        /// </summary>
        Version FileVersion { get; }

        /// <summary>
        /// True when the object has been disposed. No further operations are allowed.
        /// </summary>
        bool IsDisposed { get; }

        /// <summary>
        /// True after the file has been initialized. This property will be false right after creating a new object
        /// but before the InitializeNewFile() is called.
        /// </summary>
        bool IsInitialized { get; }

        /// <summary>
        /// Closes currently open file. This is a safe operation even on a disposed object.
        /// </summary>
        void Close();
    }
}