using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;

//
// All declarations were taken from http://www.pinvoke.net/index.aspx
//

namespace NYurik.FastBinTimeseries
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

    internal class Win32Apis
    {
        internal static SafeMapHandle CreateFileMapping(FileStream fileStream, long fileSize, FileMapProtection protection)
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

        internal static SafeMapViewHandle MapViewOfFile(SafeMapHandle hMap, long fileOffset, long mapViewSize, FileMapAccess desiredAccess)
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
            ThrowOnError(
                ReadFile(
                    fileHandle.SafeFileHandle, byteBufPtr, (uint) byteCount, out bytesProcessed, null)
                );
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
            ThrowOnError(
                WriteFile(
                    fileHandle.SafeFileHandle, byteBufPtr, (uint) byteCount, out bytesProcessed, null));
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

        private static void ThrowOnError(bool result)
        {
            if (!result)
                throw new Win32Exception(Marshal.GetLastWin32Error());
        }
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
            return Win32Apis.UnmapViewOfFile(handle);
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
            return Win32Apis.CloseHandle(handle);
        }
    }
}