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
using NYurik.TimeSeriesDb.EmitExtensions;

namespace NYurik.TimeSeriesDb.Serializers
{
    /// <summary>
    /// Use this attribute to specify custom <see cref="IBinSerializer"/> for this type
    /// </summary>
    [AttributeUsage(AttributeTargets.Struct | AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public sealed class BinarySerializerAttribute : Attribute
    {
        private readonly Type _binSerializerType;
        private readonly Type _itemType;

        public BinarySerializerAttribute(Type binSerializerType)
        {
            if (binSerializerType == null)
                throw new ArgumentNullException("binSerializerType");

            _itemType = binSerializerType.GetInterfaces().FindGenericArgument1(typeof (IBinSerializer<>));

            if (_itemType == null)
                throw new ArgumentOutOfRangeException(
                    "binSerializerType", binSerializerType,
                    "Type does not implement IBinSerializer<T>");

            _binSerializerType = binSerializerType;
        }

        public Type BinSerializerType
        {
            get { return _binSerializerType; }
        }

        public Type ItemType
        {
            get { return _itemType; }
        }
    }
}