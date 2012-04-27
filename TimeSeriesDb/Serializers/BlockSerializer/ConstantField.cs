using System;
using System.IO;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.TimeSeriesDb.Serializers.BlockSerializer
{
    public class ConstantField : BaseField
    {
        private object _value;

        [UsedImplicitly]
        protected ConstantField()
        {
        }

        public ConstantField([NotNull] IStateStore serializer, [NotNull] Type fieldType, string stateName)
            : base(Versions.Ver0, serializer, fieldType, stateName)
        {
            // default(T)
            _value = FieldType.IsValueType ? Activator.CreateInstance(FieldType) : null;
        }

        public override int MaxByteSize
        {
            get { return 0; }
        }

        /// <summary> Constant value </summary>
        public object Value
        {
            get { return _value; }
            set
            {
                ThrowOnInitialized();

                if (value == null)
                {
                    if (!FieldType.IsClass && Nullable.GetUnderlyingType(FieldType) == null)
                        throw new ArgumentNullException(
                            "value", "Null cannot be assigned to a constant field of type " + FieldType.ToDebugStr());
                }
                else
                {
                    var fldType = Nullable.GetUnderlyingType(FieldType) ?? FieldType;
                    _value = Convert.ChangeType(value, fldType);
                }
            }
        }

        protected override Tuple<Expression, Expression> GetSerializerExp(Expression valueExp, Expression codec)
        {
            var writeMethod = Expression.Constant(true);
            return new Tuple<Expression, Expression>(writeMethod, writeMethod);
        }

        protected override Tuple<Expression, Expression> GetDeSerializerExp(Expression codec)
        {
            var val = Expression.Constant(_value, FieldType);
            return new Tuple<Expression, Expression>(val, val);
        }

        protected override void InitNewField(BinaryWriter writer)
        {
            base.InitNewField(writer);

            if (_value == null)
                writer.Write(true);
            else
            {
                writer.Write(false);

                var fldType = Nullable.GetUnderlyingType(FieldType) ?? FieldType;
                switch (fldType.GetTypeCode())
                {
                    case TypeCode.Boolean:
                        writer.Write((bool) _value);
                        break;
                    case TypeCode.Char:
                        writer.Write((char) _value);
                        break;
                    case TypeCode.SByte:
                        writer.Write((sbyte) _value);
                        break;
                    case TypeCode.Byte:
                        writer.Write((byte) _value);
                        break;
                    case TypeCode.Int16:
                        writer.Write((short) _value);
                        break;
                    case TypeCode.UInt16:
                        writer.Write((ushort) _value);
                        break;
                    case TypeCode.Int32:
                        writer.Write((int) _value);
                        break;
                    case TypeCode.UInt32:
                        writer.Write((uint) _value);
                        break;
                    case TypeCode.Int64:
                        writer.Write((long) _value);
                        break;
                    case TypeCode.UInt64:
                        writer.Write((ulong) _value);
                        break;
                    case TypeCode.Single:
                        writer.Write((float) _value);
                        break;
                    case TypeCode.Double:
                        writer.Write((double) _value);
                        break;
                    case TypeCode.Decimal:
                        writer.Write((decimal) _value);
                        break;
                    case TypeCode.DateTime:
                        writer.Write(((DateTime) _value).ToBinary());
                        break;
                    case TypeCode.String:
                        writer.Write((string) _value);
                        break;
                    default:
                        throw new SerializerException("Unsupported field type {0}", FieldType.ToDebugStr());
                }
            }
        }

        protected override void InitExistingField(BinaryReader reader, Func<string, Type> typeResolver)
        {
            base.InitExistingField(reader, typeResolver);
            if (reader.ReadBoolean())
                _value = null;
            else
            {
                switch (FieldType.GetTypeCode())
                {
                    case TypeCode.Boolean:
                        _value = reader.ReadBoolean();
                        break;
                    case TypeCode.Char:
                        _value = reader.ReadChar();
                        break;
                    case TypeCode.SByte:
                        _value = reader.ReadSByte();
                        break;
                    case TypeCode.Byte:
                        _value = reader.ReadByte();
                        break;
                    case TypeCode.Int16:
                        _value = reader.ReadInt16();
                        break;
                    case TypeCode.UInt16:
                        _value = reader.ReadUInt16();
                        break;
                    case TypeCode.Int32:
                        _value = reader.ReadInt32();
                        break;
                    case TypeCode.UInt32:
                        _value = reader.ReadUInt32();
                        break;
                    case TypeCode.Int64:
                        _value = reader.ReadInt64();
                        break;
                    case TypeCode.UInt64:
                        _value = reader.ReadUInt64();
                        break;
                    case TypeCode.Single:
                        _value = reader.ReadSingle();
                        break;
                    case TypeCode.Double:
                        _value = reader.ReadDouble();
                        break;
                    case TypeCode.Decimal:
                        _value = reader.ReadDecimal();
                        break;
                    case TypeCode.DateTime:
                        _value = DateTime.FromBinary(reader.ReadInt64());
                        break;
                    case TypeCode.String:
                        _value = reader.ReadString();
                        break;
                    default:
                        throw new SerializerException("Unsupported field type {0}", FieldType.ToDebugStr());
                }
            }
        }

        protected override bool IsValidVersion(Version ver)
        {
            return ver == Versions.Ver0;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                // ReSharper disable NonReadonlyFieldInGetHashCode
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ (_value == null ? 0 : _value.GetHashCode());
                return hashCode;
                // ReSharper restore NonReadonlyFieldInGetHashCode
            }
        }

        protected override bool Equals(BaseField baseOther)
        {
            var other = (ConstantField) baseOther;
            return Equals(_value, other._value);
        }
    }
}