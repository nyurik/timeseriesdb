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
using System.IO;
using System.Reflection;
using NYurik.FastBinTimeseries.EmitExtensions;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class SubFieldInfo
    {
        public SubFieldInfo(PropertyInfo property, BaseField field)
            : this((MemberInfo) property, field)
        {
        }

        public SubFieldInfo(FieldInfo property, BaseField field)
            : this((MemberInfo) property, field)
        {
        }

        private SubFieldInfo(MemberInfo member, BaseField field)
        {
            MemberInfo = member;
            Field = field;
        }

        public SubFieldInfo(IStateStore stateStore, BinaryReader reader, Func<string, Type> typeResolver)
        {
            string typeName;
            int fixedBufferSize;
            Type type = reader.ReadType(typeResolver, out typeName, out fixedBufferSize);

            string name = reader.ReadString();
            MemberInfo mmbr = type.GetProperty(name, TypeExtensions.AllInstanceMembers) ??
                              (MemberInfo) type.GetField(name, TypeExtensions.AllInstanceMembers);
            if (mmbr == null)
                throw new SerializerException(
                    "Unable to locate the field or property {0} on type {1}", name, type.AssemblyQualifiedName);

            Type fldType = reader.ReadType(typeResolver, out typeName, out fixedBufferSize);

            Type actualFldPropType = GetFieldOrPropertyType(mmbr);
            if (actualFldPropType != fldType)
                throw new SerializerException(
                    "The type of {0} {1} on type {2} was expected to be {3}, but was {4}",
                    mmbr is FieldInfo ? "field" : "property",
                    name, type.AssemblyQualifiedName, fldType, actualFldPropType);

            MemberInfo = mmbr;

            Field = BaseField.FieldFromReader(stateStore, reader, typeResolver);
        }

        public MemberInfo MemberInfo { get; private set; }

        public BaseField Field { get; private set; }

        public void InitNew(BinaryWriter writer)
        {
            if (MemberInfo.DeclaringType == null)
                throw new SerializerException("MemberInfo {0} does not have a declaring type", MemberInfo);

            writer.WriteType(MemberInfo.DeclaringType);
            writer.Write(MemberInfo.Name);
            writer.WriteType(GetFieldOrPropertyType(MemberInfo));

            Field.InitNew(writer);
        }

        private static Type GetFieldOrPropertyType(MemberInfo memberInfo)
        {
            return memberInfo is FieldInfo
                       ? ((FieldInfo) memberInfo).FieldType
                       : ((PropertyInfo) memberInfo).PropertyType;
        }

        public override string ToString()
        {
            return string.Format("{0} {1}", MemberInfo.Name, Field);
        }
    }
}