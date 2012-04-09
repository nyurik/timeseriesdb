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
using JetBrains.Annotations;

namespace NYurik.TimeSeriesDb.Serializers.BlockSerializer
{
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Class | AttributeTargets.Struct, Inherited = false,
        AllowMultiple = false)]
    public sealed class FieldAttribute : Attribute
    {
        public FieldAttribute()
        {
        }

        public FieldAttribute(Type serializer)
        {
            Serializer = serializer;
        }

        public Type Serializer { get; private set; }
    }

    [AttributeUsage(AttributeTargets.Constructor, Inherited = false, AllowMultiple = false)]
    public class CtorFieldAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Parameter, Inherited = false, AllowMultiple = false)]
    public class CtorFieldMapToAttribute : Attribute
    {
        public CtorFieldMapToAttribute([NotNull] string fieldOrPropertyName)
        {
            if (string.IsNullOrWhiteSpace(fieldOrPropertyName)) throw new ArgumentNullException("fieldOrPropertyName");
            FieldOrPropertyName = fieldOrPropertyName;
        }

        public string FieldOrPropertyName { get; private set; }
    }
}