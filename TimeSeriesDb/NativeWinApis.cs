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
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;

//
// All declarations were adapted from http://www.pinvoke.net/index.aspx
//

namespace NYurik.TimeSeriesDb
{
    [Flags]
    internal enum FileMapProtection : uint
    {
        None = 0x00,
        PageReadOnly = 0x02,
        PageReadWrite = 0x04,
        PageWriteCopy = 0x08,
    }

    [Flags]
    internal enum FileMapAccess : uint
    {
        Copy = 0x01,
        Write = 0x02,
        Read = 0x04,
        AllAccess = 0x1f,
    }

    internal static class NativeWinApis
    {
        // ReSharper disable InconsistentNaming
        public static readonly unsafe bool Is64bit = sizeof (void*) == sizeof (long);
        // ReSharper restore InconsistentNaming
        private static SYSTEM_INFO? _sysInfo;

        public static SYSTEM_INFO SystemInfo
        {
            get
            {
                if (!_sysInfo.HasValue)
                    lock (typeof (NativeWinApis))
                        if (!_sysInfo.HasValue)
                        {
                            SYSTEM_INFO info;
                            GetNativeSystemInfo(out info);
                            _sysInfo = info;
                        }
                return _sysInfo.Value;
            }
        }

        [DllImport("kernel32.dll", CharSet = CharSet.Auto)]
        public static extern void GetNativeSystemInfo([MarshalAs(UnmanagedType.Struct)] out SYSTEM_INFO lpSystemInfo);

        internal static SafeMapHandle CreateFileMapping(
            FileStream fileStream, long fileSize,
            FileMapProtection protection)
        {
            return
                ThrowOnError(
                    CreateFileMapping(
                        fileStream.SafeFileHandle,
                        IntPtr.Zero,
                        protection,
                        (uint) ((fileSize >> 32) & 0xFFFFFFFF),
                        (uint) (fileSize & 0xFFFFFFFF),
                        null));
        }

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        private static extern SafeMapHandle CreateFileMapping(
            SafeFileHandle hFile,
            IntPtr lpFileMappingAttributes,
            FileMapProtection flProtect,
            uint dwMaximumSizeHigh,
            uint dwMaximumSizeLow,
            [MarshalAs(UnmanagedType.LPTStr)] string lpName);

        internal static SafeMapViewHandle MapViewOfFile(
            SafeMapHandle hMap, long fileOffset, long mapViewSize,
            FileMapAccess desiredAccess)
        {
            if (hMap == null || hMap.IsInvalid)
                throw new ArgumentNullException("hMap");
            if (fileOffset < 0)
                throw new ArgumentOutOfRangeException("fileOffset", fileOffset, "Must be >= 0");
            if (mapViewSize <= 0)
                throw new ArgumentOutOfRangeException("mapViewSize", mapViewSize, "Must be > 0");

            return
                ThrowOnError(
                    MapViewOfFile(
                        hMap,
                        desiredAccess,
                        (uint) ((fileOffset >> 32) & 0xFFFFFFFF),
                        (uint) (fileOffset & 0xFFFFFFFF),
                        (IntPtr) mapViewSize));
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern SafeMapViewHandle MapViewOfFile(
            SafeMapHandle hFileMappingObject,
            FileMapAccess dwDesiredAccess,
            uint dwFileOffsetHigh,
            uint dwFileOffsetLow,
            IntPtr dwNumberOfBytesToMap);

        /// <summary>
        /// Unmap file - we cannot use SafeHandle objects because they may already be disposed
        /// </summary>
        [DllImport("kernel32", SetLastError = true)]
        public static extern bool UnmapViewOfFile(IntPtr lpBaseAddress);

        /// <summary>
        /// Close handle - we cannot use SafeHandle objects because they may already be disposed
        /// </summary>
        [DllImport("kernel32", SetLastError = true)]
        public static extern bool CloseHandle(IntPtr handle);

        internal static unsafe uint ReadFile(FileStream fileHandle, byte* byteBufPtr, int byteCount)
        {
            uint bytesProcessed;
            if (!ReadFile(fileHandle.SafeFileHandle, byteBufPtr, (uint) byteCount, out bytesProcessed, null))
                throw new Win32Exception(Marshal.GetLastWin32Error());
            return bytesProcessed;
        }

        [DllImport("kernel32", SetLastError = true)]
        private static extern unsafe bool ReadFile(
            SafeFileHandle hFile,
            void* pBuffer,
            uint numBytesToRead,
            out uint numBytesRead,
            [In] NativeOverlapped* pOverlapped
            );

        internal static unsafe uint WriteFile(FileStream fileHandle, byte* byteBufPtr, int byteCount)
        {
            uint bytesProcessed;
            if (!WriteFile(fileHandle.SafeFileHandle, byteBufPtr, (uint) byteCount, out bytesProcessed, null))
                throw new Win32Exception(Marshal.GetLastWin32Error());
            return bytesProcessed;
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern unsafe bool WriteFile(
            SafeFileHandle hFile,
            void* pBuffer,
            uint numBytesToWrite,
            out uint numBytesWritten,
            [In] NativeOverlapped* pOverlapped);

        private static T ThrowOnError<T>(T handle) where T : SafeHandle
        {
            if (handle.IsInvalid)
                throw new Win32Exception(Marshal.GetLastWin32Error());
            return handle;
        }

        #region Nested type: SYSTEM_INFO

        [StructLayout(LayoutKind.Sequential)]
        // ReSharper disable InconsistentNaming
        internal struct SYSTEM_INFO
        {
            public PROCESSOR_INFO_UNION ProcessorInfo;
            public uint dwPageSize;
            public IntPtr lpMinimumApplicationAddress;
            public IntPtr lpMaximumApplicationAddress;
            public IntPtr dwActiveProcessorMask;
            public uint dwNumberOfProcessors;
            public uint dwProcessorType;
            public uint dwAllocationGranularity;
            public ushort dwProcessorLevel;
            public ushort dwProcessorRevision;

            public enum ProcArch : ushort
            {
                PROCESSOR_ARCHITECTURE_INTEL = 0, //32-bit
                PROCESSOR_ARCHITECTURE_IA64 = 6, //Itanium 64-bit
                PROCESSOR_ARCHITECTURE_AMD64 = 9, //Extended 64-bit
                PROCESSOR_ARCHITECTURE_UNKNOWN = 0xFFFF, //Unknown
            }

            [StructLayout(LayoutKind.Explicit)]
            internal struct PROCESSOR_INFO_UNION
            {
                [FieldOffset(0)] public uint dwOemId;
                [FieldOffset(0)] public ProcArch wProcessorArchitecture;
                [FieldOffset(2)] public ushort wReserved;
            }
        }

        #endregion

        // ReSharper restore InconsistentNaming
    }

    internal class SafeMapViewHandle : SafeHandleZeroOrMinusOneIsInvalid
    {
        private SafeMapViewHandle()
            : base(true)
        {
        }

        public long Address
        {
            get { return handle.ToInt64(); }
        }

        protected override bool ReleaseHandle()
        {
            return NativeWinApis.UnmapViewOfFile(handle);
        }
    }

    internal class SafeMapHandle : SafeHandleZeroOrMinusOneIsInvalid
    {
        private SafeMapHandle()
            : base(true)
        {
        }

        protected override bool ReleaseHandle()
        {
            return NativeWinApis.CloseHandle(handle);
        }
    }
}