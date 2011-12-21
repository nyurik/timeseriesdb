#region COPYRIGHT
/*
 *     Copyright 2009-2011 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
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
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    [Serializable]
    public class IncompatibleVersionException : FormattedException
    {
        protected IncompatibleVersionException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        public IncompatibleVersionException(Type type, Version version)
            : base("Version {0} is not supported by type {1}", version, type.AssemblyQualifiedName)
        {
        }
    }
}