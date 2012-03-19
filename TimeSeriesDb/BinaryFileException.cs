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
using System.Runtime.Serialization;
using JetBrains.Annotations;
using NYurik.TimeSeriesDb.CommonCode;

namespace NYurik.TimeSeriesDb
{
    [Serializable]
    public class BinaryFileException : FormattedException
    {
        public BinaryFileException()
        {
        }

        public BinaryFileException(string message) : base(message)
        {
        }

        [StringFormatMethod("message")]
        public BinaryFileException(string message, params object[] args) : base(message, args)
        {
        }

        public BinaryFileException(Exception inner, string message) : base(inner, message)
        {
        }

        [StringFormatMethod("message")]
        public BinaryFileException(Exception inner, string message, params object[] args) : base(inner, message, args)
        {
        }

        protected BinaryFileException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}