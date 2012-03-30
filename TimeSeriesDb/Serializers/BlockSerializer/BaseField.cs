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
using System.Diagnostics.Contracts;
using System.Globalization;
using System.IO;
using System.Linq.Expressions;
using JetBrains.Annotations;
using NYurik.TimeSeriesDb.Common;

namespace NYurik.TimeSeriesDb.Serializers.BlockSerializer
{
    public abstract class BaseField : Initializable
    {
        protected static readonly Version Version10 = new Version(1, 0);

        private string _stateName;
        private IStateStore _stateStore;
        private Version _version;

        protected BaseField()
        {
        }

        /// <param name="version"></param>
        /// <param name="stateStore"></param>
        /// <param name="valueType">Type of value to store</param>
        /// <param name="stateName">Name of the value (for debugging)</param>
        protected BaseField(
            Version version, [NotNull] IStateStore stateStore, [NotNull] Type valueType,
            string stateName = null)
        {
            if (stateStore == null) throw new ArgumentNullException("stateStore");
            if (valueType == null) throw new ArgumentNullException("valueType");

            _version = version;
            ValueType = valueType;
            _stateStore = stateStore;
            _stateName = stateName;
        }

        public Type ValueType { get; private set; }

        public TypeCode ValueTypeCode
        {
            get { return Type.GetTypeCode(ValueType); }
        }

        public IStateStore StateStore
        {
            get { return _stateStore; }
            set
            {
                ThrowOnInitialized();
                if (value == null)
                    throw new ArgumentNullException("value");
                if (_stateStore != null && !ReferenceEquals(value, _stateStore))
                    throw new ArgumentException("StateStore has already been set");
                _stateStore = value;
            }
        }

        public string StateName
        {
            get { return _stateName; }
            set
            {
                ThrowOnInitialized();
                _stateName = value;
            }
        }

        public virtual Version Version
        {
            get { return _version; }
            set
            {
                ThrowOnInitialized();
                if (!IsValidVersion(value))
                    throw new IncompatibleVersionException(GetType(), value);
                _version = value;
            }
        }

        /// <summary>
        /// Maximum total number of bytes this field (including any nested fields) might need
        /// </summary>
        public abstract int MaxByteSize { get; }

        public void InitNew(BinaryWriter writer)
        {
            EnsureReadonly();
            writer.WriteType(GetType());
            InitNewField(writer);
        }

        [Pure]
        public static BaseField FieldFromReader(
            IStateStore stateStore, BinaryReader reader,
            Func<string, Type> typeResolver)
        {
            var fld = reader.ReadTypeAndInstantiate<BaseField>(typeResolver, true);

            fld.StateStore = stateStore;
            fld.InitExistingField(reader, typeResolver);
            fld.EnsureReadonly();

            return fld;
        }

        protected virtual void InitNewField(BinaryWriter writer)
        {
            if (!IsValidVersion(Version))
                throw new IncompatibleVersionException(GetType(), Version);

            writer.WriteVersion(Version);
            writer.WriteType(ValueType);
            writer.Write(StateName);
        }

        protected virtual void InitExistingField(BinaryReader reader, Func<string, Type> typeResolver)
        {
            Version = reader.ReadVersion();
            if (!IsValidVersion(Version))
                throw new IncompatibleVersionException(GetType(), Version);

            string typeName;
            int size;
            ValueType = reader.ReadType(typeResolver, out typeName, out size);

            StateName = reader.ReadString();
        }

        protected static ConstantExpression Const(object value, Type toType = null)
        {
            return
                toType == null || (value != null && value.GetType() == toType)
                    ? Expression.Constant(value)
                    : Expression.Constant(Convert.ChangeType(value, toType, CultureInfo.InvariantCulture), toType);
        }

        protected abstract bool IsValidVersion(Version ver);

        protected static MethodCallExpression WriteSignedValue(Expression codec, Expression value)
        {
            return Expression.Call(codec, "WriteSignedValue", null, value);
        }

        protected static MethodCallExpression WriteUnsignedValue(Expression codec, Expression value)
        {
            return Expression.Call(codec, "WriteUnsignedValue", null, value);
        }

        protected static MethodCallExpression ReadSignedValue(Expression codec)
        {
            return Expression.Call(codec, "ReadSignedValue", null);
        }

        protected static MethodCallExpression ReadUnsignedValue(Expression codec)
        {
            return Expression.Call(codec, "ReadUnsignedValue", null);
        }

        protected static MethodCallExpression ThrowOverflow(Expression codec, [NotNull] Expression value)
        {
            if (value == null) throw new ArgumentNullException("value");
            return Expression.Call(codec, "ThrowOverflow", new[] {value.Type}, value);
        }

        protected static MethodCallExpression ThrowSerializer(Expression codec, string format, params Expression[] args)
        {
            return Expression.Call(codec, "ThrowSerializer", null, ToFormatArgs(format, args));
        }

        private static Expression[] ToFormatArgs(string format, params Expression[] arguments)
        {
            var res = new List<Expression> {Const(format)};
            foreach (Expression arg in arguments)
                res.Add(
                    arg.Type != typeof (object)
                        ? Expression.Convert(arg, typeof (object))
                        : arg);
            return res.ToArray();
        }

        protected internal static Expression DebugValueExp(Expression codec, Expression value, string name)
        {
#if DEBUG_SERIALIZER
            string methodName;
            Type destType;
            switch (Type.GetTypeCode(value.Type))
            {
                case TypeCode.Boolean:
                case TypeCode.Char:
                case TypeCode.SByte:
                case TypeCode.Byte:
                case TypeCode.Int16:
                case TypeCode.UInt16:
                case TypeCode.Int32:
                case TypeCode.UInt32:
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    methodName = "DebugLong";
                    destType = typeof (long);
                    break;
                case TypeCode.Single:
                    methodName = "DebugFloat";
                    destType = typeof (float);
                    break;
                case TypeCode.Double:
                    methodName = "DebugDouble";
                    destType = typeof (double);
                    break;
                default:
                    throw new ArgumentOutOfRangeException("value", value.Type, "Unknown type");
            }

            Expression posExp = Expression.PropertyOrField(
                codec, codec.Type == typeof (CodecWriter) ? "Count" : "BufferPos");

            var prm = value as ParameterExpression;
            return Expression.Call(
                codec, methodName, null,
                value.Type == destType ? value : Expression.Convert(value, destType),
                posExp,
                Expression.Constant(name + (prm != null ? " " + prm.Name : null)));
#else
            return Expression.Empty();
#endif
        }

        protected virtual void MakeReadonly()
        {
            IsInitialized = true;
        }

        public Tuple<Expression, Expression> GetSerializer([NotNull] Expression valueExp, [NotNull] Expression codec)
        {
            if (valueExp == null) throw new ArgumentNullException("valueExp");
            if (codec == null) throw new ArgumentNullException("codec");
            EnsureReadonly();

            if (ValueType != valueExp.Type)
                throw new SerializerException(
                    "Serializer received an unexpected value of type {0}, instead of {1}",
                    valueExp.Type.FullName, ValueType.FullName);

            Tuple<Expression, Expression> srl = GetSerializerExp(valueExp, codec);

            if (srl.Item2.Type != typeof (bool))
                throw new SerializerException(
                    "Serializer 'next' has an unexpected result type {0}, instead of a boolean",
                    srl.Item2.Type.FullName);

            return srl;
        }

        public Tuple<Expression, Expression> GetDeSerializer(Expression codec)
        {
            EnsureReadonly();

            Tuple<Expression, Expression> srl = GetDeSerializerExp(codec);

            if (ValueType != srl.Item1.Type)
                throw new SerializerException(
                    "DeSerializer 'init' has unexpected type {0}, instead of {1}",
                    srl.Item1.Type.FullName, ValueType.FullName);

            if (ValueType != srl.Item2.Type)
                throw new SerializerException(
                    "DeSerializer 'next' has unexpected type {0}, instead of {1}",
                    srl.Item2.Type.FullName, ValueType.FullName);

            return srl;
        }

        protected abstract Tuple<Expression, Expression> GetSerializerExp(
            [NotNull] Expression valueExp, [NotNull] Expression codec);

        protected abstract Tuple<Expression, Expression> GetDeSerializerExp([NotNull] Expression codec);

        private void EnsureReadonly()
        {
            if (!IsInitialized)
                MakeReadonly();
            if (!IsInitialized)
                throw new SerializerException(
                    "Derived serializer {0} must call base when validating", GetType().AssemblyQualifiedName);
        }

        public override string ToString()
        {
            return string.Format("{0} {1}", GetType().Name, StateName);
        }

        /// <summary>
        /// Every field must override 
        /// <see cref="GetHashCode"/> and  <see cref="Equals(NYurik.TimeSeriesDb.Serializers.BlockSerializer.BaseField)"/>,
        /// and combine base call with its own results.
        /// </summary>
        protected abstract bool Equals(BaseField baseOther);

        /// <summary>
        /// Every field must override 
        /// <see cref="GetHashCode"/> and  <see cref="Equals(NYurik.TimeSeriesDb.Serializers.BlockSerializer.BaseField)"/>,
        /// and combine base call with its own results.
        /// </summary>
        public override int GetHashCode()
        {
            unchecked
            {
                // ReSharper disable NonReadonlyFieldInGetHashCode
                int hashCode = _stateName.GetHashCode();
                hashCode = (hashCode*397) ^ ValueType.GetHashCode();
                hashCode = (hashCode*397) ^ _version.GetHashCode();
                // ReSharper restore NonReadonlyFieldInGetHashCode
                return hashCode;
            }
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;

            var other = (BaseField) obj;
            return string.Equals(_stateName, other._stateName)
                   && ValueType == other.ValueType
                   && Equals(_version, other._version)
                   && Equals(other);
        }
    }
}