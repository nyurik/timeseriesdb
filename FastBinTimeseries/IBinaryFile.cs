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

namespace NYurik.FastBinTimeseries
{
    public interface IBinaryFile : IGenericInvoker, IDisposable
    {
        /// <summary> Type of the items stored in this file </summary>
        Type ItemType { get; }

        /// <summary> User string stored in the header </summary>
        string Tag { get; }

        /// <summary> True when the file has no data </summary>
        bool IsEmpty { get; }

        /// <summary> True when the object has been disposed. No further operations are allowed. </summary>
        bool IsDisposed { get; }

        /// <summary> Access to the non-generic instance of the current serializer </summary>
        IBinSerializer NonGenericSerializer { get; }

        /// <summary>True if the file is ready for read/write operations </summary>
        bool IsOpen { get; }

        /// <summary> Can be changed at any time. Enables MMF access mode when reading from a file. </summary>
        bool EnableMemMappedAccessOnRead { get; set; }

        /// <summary> Can be changed at any time. Enables MMF access mode when writing to a file. </summary>
        bool EnableMemMappedAccessOnWrite { get; set; }

        /// <summary> Full path to the file </summary>
        string FileName { get; }

        /// <summary> Base version of the serializer that was used to create this file </summary>
        Version BaseVersion { get; }

        /// <summary> The size of each item of data in bytes </summary>
        int ItemSize { get; }

        /// <summary> Size of the file header in bytes </summary>
        int HeaderSize { get; }

        /// <summary> Was file open for writing </summary>
        bool CanWrite { get; }

        /// <summary> The version of the binary file handler used to create this file </summary>
        Version Version { get; }

        /// <summary>
        /// True after the file has been initialized. This property will be false right after creating a new object
        /// but before the InitializeNewFile() is called.
        /// </summary>
        bool IsInitialized { get; }

        /// <summary> Total number of items in the file </summary>
        long GetItemCount();

        /// <summary> Closes currently open file. This is a safe operation even on a disposed object. </summary>
        void Close();
    }
}