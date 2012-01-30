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