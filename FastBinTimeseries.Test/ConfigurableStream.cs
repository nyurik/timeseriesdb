using System;
using System.IO;

namespace NYurik.FastBinTimeseries.Test
{
    public class ConfigurableStream : Stream
    {
        private readonly Stream _stream;

        public ConfigurableStream(Stream stream)
        {
            _stream = stream;
            AllowSeek = true;
            AllowRead = true;
            AllowWrite = true;
        }

        public bool AllowSeek { get; set; }
        public bool AllowRead { get; set; }
        public bool AllowWrite { get; set; }


        public override bool CanRead
        {
            get { return AllowRead && _stream.CanRead; }
        }

        public override bool CanSeek
        {
            get { return AllowSeek && _stream.CanSeek; }
        }

        public override bool CanWrite
        {
            get { return AllowWrite && _stream.CanWrite; }
        }

        public override long Length
        {
            get
            {
                RequestSeek();
                return _stream.Length;
            }
        }

        public override long Position
        {
            get
            {
                RequestSeek();
                return _stream.Position;
            }
            set
            {
                RequestSeek();
                _stream.Position = value;
            }
        }

        private void RequestSeek()
        {
            if (!AllowSeek) throw new NotSupportedException();
        }

        private void RequestRead()
        {
            if (!AllowRead) throw new NotSupportedException();
        }

        private void RequestWrite()
        {
            if (!AllowWrite) throw new NotSupportedException();
        }

        public override void Flush()
        {
            _stream.Flush();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            RequestSeek();
            return _stream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            RequestSeek();
            _stream.SetLength(value);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            RequestRead();
            return _stream.Read(buffer, offset, count);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            RequestWrite();
            _stream.Write(buffer, offset, count);
        }
    }
}