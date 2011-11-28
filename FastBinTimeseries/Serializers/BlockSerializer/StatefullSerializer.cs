using System;
using JetBrains.Annotations;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class StatefullSerializer : Initializable
    {
        public static BaseSerializer GetSerializer([NotNull] Type valueType, string name = null)
        {
            if (valueType.IsArray)
                throw new SerializerException("Arrays are not supported ({0})", valueType);

            if (valueType.IsPrimitive)
            {
                switch (Type.GetTypeCode(valueType))
                {
                    case TypeCode.SByte:
                    case TypeCode.Byte:
                        return new SimpleSerializer(valueType, name);

                    case TypeCode.Char:
                    case TypeCode.Int16:
                    case TypeCode.UInt16:
                    case TypeCode.Int32:
                    case TypeCode.UInt32:
                    case TypeCode.Int64:
                    case TypeCode.UInt64:
                    case TypeCode.Single:
                    case TypeCode.Double:
                    case TypeCode.Decimal:
                        return new MultipliedDeltaSerializer(valueType, name);

                    default:
                        throw new SerializerException("Unsupported primitive type {0}", valueType);
                }
            }

            if (valueType == typeof (UtcDateTime))
                return new UtcDateTimeSerializer(name);

            return new FieldsSerializer(valueType, name);
        }
    }
}