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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public static class LegacySupport
    {
        private static readonly Assembly MainAssembly = typeof (BinaryFile).Assembly;

        private static readonly AssemblyName MainAssemblyName = MainAssembly.GetName();

        private static readonly Dictionary<string, Type> LegacyTypes;

        static LegacySupport()
        {
            // public, non-abstract, non-static class
            LegacyTypes =
                (from type in typeof (LegacySupport).Assembly.GetTypes()
                 where type.IsPublic && !type.IsAbstract
                 select type).ToDictionary(i => i.FullName);
        }

        public static Type TypeResolver(string typeName)
        {
            return TypeUtils.TypeResolver(typeName, TypeResolver, TypeUtils.DefaultTypeResolver);
        }

        public static Type TypeResolver(TypeSpec spec, AssemblyName assemblyName)
        {
            Type type;
            if (assemblyName != null && assemblyName.Name == MainAssemblyName.Name &&
                LegacyTypes.TryGetValue(spec.Name, out type))
                return type;

            return null;
        }
    }
}