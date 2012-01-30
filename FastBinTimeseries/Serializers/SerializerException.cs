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
using System.Runtime.Serialization;
using JetBrains.Annotations;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries.Serializers
{
    [Serializable]
    public class SerializerException : FormattedException
    {
        public SerializerException()
        {
        }

        public SerializerException(string message) : base(message)
        {
        }

        [StringFormatMethod("message")]
        public SerializerException(string message, params object[] args) : base(message, args)
        {
        }

        public SerializerException(Exception inner, string message) : base(inner, message)
        {
        }

        [StringFormatMethod("message")]
        public SerializerException(Exception inner, string message, params object[] args) : base(inner, message, args)
        {
        }

        protected SerializerException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}