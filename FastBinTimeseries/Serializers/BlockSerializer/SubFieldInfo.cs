using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using NYurik.EmitExtensions;

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

        public SubFieldInfo(BinaryReader reader, IDictionary<string, Type> typeMap)
        {
            string typeName;
            bool typeRemapped;
            int fixedBufferSize;
            Type type = reader.ReadType(typeMap, out typeName, out typeRemapped, out fixedBufferSize);

            string name = reader.ReadString();
            MemberInfo mmbr = type.GetProperty(name, TypeExtensions.AllInstanceMembers) ??
                              (MemberInfo) type.GetField(name, TypeExtensions.AllInstanceMembers);
            if (mmbr == null)
                throw new SerializerException(
                    "Unable to locate the field or property {0} on type {1}", name, type.AssemblyQualifiedName);

            Type fldType = reader.ReadType(typeMap, out typeName, out typeRemapped, out fixedBufferSize);
            
            Type actualFldPropType = GetFieldOrPropertyType(mmbr);
            if (actualFldPropType != fldType)
                throw new SerializerException(
                    "The type of {0} {1} on type {2} was expected to be {3}, but was {4}",
                    mmbr is FieldInfo ? "field" : "property",
                    name, type.AssemblyQualifiedName, fldType, actualFldPropType);

            MemberInfo = mmbr;

            Field = reader.ReadTypeAndInstantiate<BaseField>(typeMap, true);
            Field.InitExisting(reader, typeMap);
        }

        public MemberInfo MemberInfo { get; private set; }
        public BaseField Field { get; private set; }

        public void Validate()
        {
            Field.Validate();
        }

        public void InitNew(BinaryWriter writer)
        {
            writer.WriteType(MemberInfo.DeclaringType);
            writer.Write(MemberInfo.Name);
            writer.WriteType(GetFieldOrPropertyType(MemberInfo));

            writer.WriteType(Field);
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