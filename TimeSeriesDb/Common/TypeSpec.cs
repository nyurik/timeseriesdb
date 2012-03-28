//
// System.Type.cs
//
// Author:
//   Rodrigo Kumpera <kumpera@gmail.com>
//
//
// Copyright (C) 2010 Novell, Inc (http://www.novell.com)
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//

//
// The code has been refactored from the original by Yuri Astrakhan @ gmail.com
// Taken from https://github.com/mono/mono/blob/master/mcs/class/corlib/System/TypeSpec.cs on 2012-01-28
//

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Reflection;
using System.Text;
using JetBrains.Annotations;

namespace NYurik.TimeSeriesDb.Common
{
    public class TypeSpec
    {
        private TypeSpec(string originalTypeString)
        {
            OriginalTypeString = originalTypeString;
        }

        public string OriginalTypeString { get; private set; }
        public string Name { get; private set; }
        public string AssemblyName { get; private set; }
        public IList<string> Nested { get; private set; }
        public IList<TypeSpec> GenericParams { get; private set; }
        public IList<ArraySpec> ArraySpecs { get; private set; }
        public int PointerLevel { get; private set; }
        public bool IsByRef { get; private set; }
        public Type ResolvedType { get; private set; }

        private bool IsArray
        {
            get { return ArraySpecs != null; }
        }

        public override string ToString()
        {
            var str = new StringBuilder(Name);
            if (Nested != null)
            {
                foreach (string n in Nested)
                {
                    str.Append("+");
                    str.Append(n);
                }
            }

            if (GenericParams != null)
            {
                str.Append("[");
                for (int i = 0; i < GenericParams.Count; ++i)
                {
                    if (i > 0)
                        str.Append(", ");
                    if (GenericParams[i].AssemblyName != null)
                    {
                        str.Append("[");
                        str.Append(GenericParams[i]);
                        str.Append("]");
                    }
                    else
                        str.Append(GenericParams[i]);
                }
                str.Append("]");
            }

            if (ArraySpecs != null)
            {
                foreach (ArraySpec ar in ArraySpecs)
                    str.Append(ar);
            }

            for (int i = 0; i < PointerLevel; ++i)
                str.Append("*");

            if (IsByRef)
                str.Append("&");

            if (AssemblyName != null)
            {
                str.Append(", ");
                str.Append(AssemblyName);
            }

            return str.ToString();
        }

        public static TypeSpec Parse(string typeName)
        {
            int pos = 0;
            if (typeName == null)
                throw new ArgumentNullException("typeName");

            TypeSpec res = ParseAndMakeReadonly(typeName, ref pos, false, false);
            if (pos < typeName.Length)
                throw new ArgumentException("Count not parse the whole type name", "typeName");

            return res;
        }

        public Type Resolve([NotNull] params Func<TypeSpec, AssemblyName, Type>[] typeResolvers)
        {
            return Resolve(ts => DefaultFullTypeResolver(ts, typeResolvers));
        }

        public Type Resolve([NotNull] params Func<TypeSpec, Type>[] fullTypeResolvers)
        {
            if (fullTypeResolvers == null || fullTypeResolvers.Length == 0)
                throw new ArgumentNullException("fullTypeResolvers");

            if (ResolvedType != null)
                return ResolvedType;

            if (GenericParams != null)
                foreach (TypeSpec gp in GenericParams)
                    if (gp.Resolve(fullTypeResolvers) == null)
                        return null;

            Type type = null;
            foreach (var ftr in fullTypeResolvers)
            {
                type = ftr(this);
                if (type != null)
                    break;
            }
            if (type == null)
                return null;

            if (ArraySpecs != null)
            {
                foreach (ArraySpec arr in ArraySpecs)
                    type = arr.Resolve(type);
            }

            for (int i = 0; i < PointerLevel; ++i)
                type = type.MakePointerType();

            if (IsByRef)
                type = type.MakeByRefType();

            ResolvedType = type;

            return type;
        }

        /// <summary>
        /// Resolves TypeSpec into a specific type (including subtypes and generic parameters)
        /// </summary>
        /// <param name="spec">Type info with all generics already resolved</param>
        /// <param name="typeResolvers">Resolver to convert a specific type without generic parameters and without subtypes</param>
        public static Type DefaultFullTypeResolver(
            [NotNull] TypeSpec spec, [NotNull] params Func<TypeSpec, AssemblyName, Type>[] typeResolvers)
        {
            if (spec == null) throw new ArgumentNullException("spec");
            if (typeResolvers == null || typeResolvers.Length == 0) throw new ArgumentNullException("typeResolvers");

            AssemblyName assemblyName = spec.AssemblyName == null ? null : new AssemblyName(spec.AssemblyName);
            Type type = null;

            foreach (var tr in typeResolvers)
            {
                type = tr(spec, assemblyName);
                if (type != null)
                    break;
            }

            if (type == null)
                return null;

            if (spec.Nested != null)
            {
                foreach (string n in spec.Nested)
                {
                    Type tmp = type.GetNestedType(n, BindingFlags.Public | BindingFlags.NonPublic);
                    if (tmp == null)
                        throw new TypeLoadException("Could not find nested type '" + n + "'");
                    type = tmp;
                }
            }

            if (spec.GenericParams != null)
            {
                var args = new Type[spec.GenericParams.Count];
                for (int i = 0; i < args.Length; ++i)
                    args[i] = spec.GenericParams[i].ResolvedType;
                type = type.MakeGenericType(args);
            }
            return type;
        }

        private void AddName(string typeName)
        {
            if (Name == null)
            {
                Name = typeName;
            }
            else
            {
                if (Nested == null)
                    Nested = new List<string>();
                Nested.Add(typeName);
            }
        }

        private void AddArray(ArraySpec array)
        {
            if (ArraySpecs == null)
                ArraySpecs = new List<ArraySpec>();
            ArraySpecs.Add(array);
        }

        private static void SkipSpace(string name, ref int pos)
        {
            int p = pos;
            while (p < name.Length && Char.IsWhiteSpace(name[p]))
                ++p;
            pos = p;
        }

        private static TypeSpec ParseAndMakeReadonly(string typeName, ref int p, bool isRecurse, bool allowAqn)
        {
            TypeSpec tmp = Parse(typeName, ref p, isRecurse, allowAqn);
            if (tmp.GenericParams != null)
                tmp.GenericParams = new ReadOnlyCollection<TypeSpec>(tmp.GenericParams);
            if (tmp.Nested != null)
                tmp.Nested = new ReadOnlyCollection<string>(tmp.Nested);
            if (tmp.ArraySpecs != null)
                tmp.ArraySpecs = new ReadOnlyCollection<ArraySpec>(tmp.ArraySpecs);
            return tmp;
        }

        private static TypeSpec Parse(string typeName, ref int p, bool isRecurse, bool allowAqn)
        {
            int pos = p;
            bool inModifiers = false;
            var data = new TypeSpec(typeName);

            SkipSpace(typeName, ref pos);

            int nameStart = pos;

            for (; pos < typeName.Length; ++pos)
            {
                switch (typeName[pos])
                {
                    case '+':
                        data.AddName(typeName.Substring(nameStart, pos - nameStart));
                        nameStart = pos + 1;
                        break;
                    case ',':
                    case ']':
                        data.AddName(typeName.Substring(nameStart, pos - nameStart));
                        nameStart = pos + 1;
                        inModifiers = true;
                        if (isRecurse && !allowAqn)
                        {
                            p = pos;
                            return data;
                        }
                        break;
                    case '&':
                    case '*':
                    case '[':
                        if (typeName[pos] != '[' && isRecurse)
                            throw new ArgumentException("Generic argument can't be byref or pointer type", "typeName");
                        data.AddName(typeName.Substring(nameStart, pos - nameStart));
                        nameStart = pos + 1;
                        inModifiers = true;
                        break;
                }
                if (inModifiers)
                    break;
            }

            if (nameStart < pos)
                data.AddName(typeName.Substring(nameStart, pos - nameStart));

            if (inModifiers)
            {
                for (; pos < typeName.Length; ++pos)
                {
                    switch (typeName[pos])
                    {
                        case '&':
                            if (data.IsByRef)
                                throw new ArgumentException("Can't have a byref of a byref", "typeName");
                            data.IsByRef = true;
                            break;
                        case '*':
                            if (data.IsByRef)
                                throw new ArgumentException("Can't have a pointer to a byref type", "typeName");
                            ++data.PointerLevel;
                            break;
                        case ',':
                            if (isRecurse)
                            {
                                int end = pos;
                                while (end < typeName.Length && typeName[end] != ']')
                                    ++end;
                                if (end >= typeName.Length)
                                    throw new ArgumentException(
                                        "Unmatched ']' while parsing generic argument assembly name");
                                data.AssemblyName = typeName.Substring(pos + 1, end - pos - 1).Trim();
                                p = end + 1;
                                return data;
                            }
                            data.AssemblyName = typeName.Substring(pos + 1).Trim();
                            pos = typeName.Length;
                            break;
                        case '[':
                            if (data.IsByRef)
                                throw new ArgumentException(
                                    "Byref qualifier must be the last one of a type", "typeName");
                            ++pos;
                            if (pos >= typeName.Length)
                                throw new ArgumentException("Invalid array/generic spec", "typeName");
                            SkipSpace(typeName, ref pos);

                            if (typeName[pos] != ',' && typeName[pos] != '*' && typeName[pos] != ']')
                            {
                                //generic args
                                var args = new List<TypeSpec>();
                                if (data.IsArray)
                                    throw new ArgumentException("generic args after array spec", "typeName");

                                while (pos < typeName.Length)
                                {
                                    SkipSpace(typeName, ref pos);
                                    bool aqn = typeName[pos] == '[';
                                    if (aqn)
                                        ++pos; //skip '[' to the start of the type
                                    args.Add(ParseAndMakeReadonly(typeName, ref pos, true, aqn));
                                    if (pos >= typeName.Length)
                                        throw new ArgumentException("Invalid generic arguments spec", "typeName");

                                    if (typeName[pos] == ']')
                                        break;
                                    if (typeName[pos] == ',')
                                        ++pos; // skip ',' to the start of the next arg
                                    else
                                        throw new ArgumentException(
                                            "Invalid generic arguments separator " + typeName[pos], "typeName");
                                }
                                if (pos >= typeName.Length || typeName[pos] != ']')
                                    throw new ArgumentException("Error parsing generic params spec", "typeName");
                                data.GenericParams = args;
                            }
                            else
                            {
                                //array spec
                                int dimensions = 1;
                                bool bound = false;
                                while (pos < typeName.Length && typeName[pos] != ']')
                                {
                                    if (typeName[pos] == '*')
                                    {
                                        if (bound)
                                            throw new ArgumentException(
                                                "Array spec cannot have 2 bound dimensions", "typeName");
                                        bound = true;
                                    }
                                    else if (typeName[pos] != ',')
                                        throw new ArgumentException(
                                            "Invalid character in array spec " + typeName[pos], "typeName");
                                    else
                                        ++dimensions;

                                    ++pos;
                                    SkipSpace(typeName, ref pos);
                                }
                                if (typeName[pos] != ']')
                                    throw new ArgumentException("Error parsing array spec", "typeName");
                                if (dimensions > 1 && bound)
                                    throw new ArgumentException(
                                        "Invalid array spec, multi-dimensional array cannot be bound", "typeName");
                                data.AddArray(new ArraySpec(dimensions, bound));
                            }

                            break;
                        case ']':
                            if (isRecurse)
                            {
                                p = pos + 1;
                                return data;
                            }
                            throw new ArgumentException("Unmatched ']'", "typeName");
                        default:
                            throw new ArgumentException(
                                "Bad type def, can't handle '" + typeName[pos] + "'" + " at " + pos, "typeName");
                    }
                }
            }

            p = pos;
            return data;
        }
    }

    public class ArraySpec
    {
        internal ArraySpec(int dimensions, bool bound)
        {
            Dimensions = dimensions;
            Bound = bound;
        }

        public int Dimensions { get; private set; }
        public bool Bound { get; private set; }

        internal Type Resolve(Type type)
        {
            if (Bound)
                return type.MakeArrayType(1);
            if (Dimensions == 1)
                return type.MakeArrayType();
            return type.MakeArrayType(Dimensions);
        }

        public override string ToString()
        {
            if (Bound)
                return "[*]";
            var str = new StringBuilder("[");
            for (int i = 1; i < Dimensions; ++i)
                str.Append(",");
            str.Append("]");
            return str.ToString();
        }
    }
}