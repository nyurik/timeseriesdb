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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using NYurik.TimeSeriesDb.Common;

namespace NYurik.TimeSeriesDb
{
    public static class LegacySupport
    {
        private const string OldMainName = "NYurik.FastBinTimeseries";
        private const string OldLegacyName = "NYurik.FastBinTimeseries.Legacy";

        private static readonly Type LegacyObj = typeof(LegacySupport);
        private static readonly Assembly LegacyAssembly = LegacyObj.Assembly;

        private static readonly Type MainObj = typeof(BinaryFile);
        private static readonly Assembly MainAssembly = MainObj.Assembly;
        private static readonly AssemblyName MainAssemblyName = MainAssembly.GetName();

        private static readonly Dictionary<string, Type> MovedToLegacyTypes;
        private static readonly Dictionary<string, Type> RenamedLegacyTypes;
        private static readonly Dictionary<string, Type> RenamedMainTypes;

        private static readonly Lazy<Func<string, Type>> DefaultLegacyResolver =
            new Lazy<Func<string, Type>>(
                () =>
                TypeUtils.CreateCachingResolver(TypeResolver, TypeUtils.ResolverFromAnyAssemblyVersion));

        static LegacySupport()
        {
            // public, non-abstract, non-static class
            MovedToLegacyTypes =
                (from type in LegacyAssembly.GetTypes()
                 where type.IsPublic && !type.IsAbstract
                 select type).ToDictionary(i => i.FullName);

            // ReSharper disable PossibleNullReferenceException
            RenamedMainTypes =
                (from type in MainAssembly.GetTypes()
                 where type.IsPublic && !type.IsAbstract
                 select type)
                    .ToDictionary(i => i.FullName.Replace(MainObj.Namespace + ".", OldMainName + "."));

            RenamedLegacyTypes =
                (from type in LegacyAssembly.GetTypes()
                 where type.IsPublic && !type.IsAbstract
                 select type)
                    .ToDictionary(i => i.FullName.Replace(MainObj.Namespace + ".", OldMainName + "."));
            // ReSharper restore PossibleNullReferenceException
        }

        public static Type TypeResolver(string typeName)
        {
            return DefaultLegacyResolver.Value(typeName);
        }

        public static Type TypeResolver(TypeSpec spec, AssemblyName assemblyName)
        {
            if (assemblyName != null)
            {
                Type type;
                if (assemblyName.Name == MainAssemblyName.Name && MovedToLegacyTypes.TryGetValue(spec.Name, out type))
                    return type;
                if (assemblyName.Name == OldMainName && RenamedMainTypes.TryGetValue(spec.Name, out type))
                    return type;
                if (assemblyName.Name == OldLegacyName && RenamedLegacyTypes.TryGetValue(spec.Name, out type))
                    return type;
            }

            return null;
        }
    }
}