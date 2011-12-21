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
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public abstract class BaseField : Initializable
    {
        protected static readonly Version Version10 = new Version(1, 0);
        private string _stateName;
        private IStateStore _stateStore;

        protected BaseField()
        {
        }

        /// <param name="version"></param>
        /// <param name="stateStore"></param>
        /// <param name="valueType">Type of value to store</param>
        /// <param name="stateName">Name of the value (for debugging)</param>
        protected BaseField(Version version, [NotNull] IStateStore stateStore, [NotNull] Type valueType,
                            string stateName = null)
        {
            if (stateStore == null) throw new ArgumentNullException("stateStore");
            if (valueType == null) throw new ArgumentNullException("valueType");

            Version = version;
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

        public Version Version { get; private set; }

        public void InitNew(BinaryWriter writer)
        {
            EnsureReadonly();
            writer.WriteType(GetType());
            InitNewField(writer);
        }

        [Pure]
        public static BaseField FieldFromReader(IStateStore stateStore, BinaryReader reader,
                                                IDictionary<string, Type> typeMap)
        {
            var fld = reader.ReadTypeAndInstantiate<BaseField>(typeMap, true);

            fld.StateStore = stateStore;
            fld.InitExistingField(reader, typeMap);
            fld.EnsureReadonly();

            return fld;
        }

        public abstract int GetMinByteSize();
        public abstract int GetMaxByteSize();

        protected virtual void InitNewField(BinaryWriter writer)
        {
            writer.WriteVersion(Version);
            writer.WriteType(ValueType);
            writer.Write(StateName);
        }

        protected virtual void InitExistingField(BinaryReader reader, IDictionary<string, Type> typeMap)
        {
            Version = reader.ReadVersion();

            string typeName;
            bool remapped;
            int size;
            ValueType = reader.ReadType(typeMap, out typeName, out remapped, out size);

            StateName = reader.ReadString();
        }

        protected MethodCallExpression WriteSignedValue(Expression codec, Expression value)
        {
            return Expression.Call(codec, "WriteSignedValue", null, value);
        }

        protected MethodCallExpression ReadSignedValue(Expression codec)
        {
            return Expression.Call(codec, "ReadSignedValue", null);
        }

        protected MethodCallExpression ThrowOverflow(Expression codec, Expression value)
        {
            return Expression.Call(codec, "ThrowOverflow", new[] {value.Type}, value);
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

            Expression posExp;
            if (codec.Type == typeof (CodecWriter))
                posExp = Expression.PropertyOrField(codec, "Count");
            else
                posExp = Expression.PropertyOrField(codec, "BufferPos");

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

        public Tuple<Expression, Expression> GetSerializer(Expression valueExp, Expression codec)
        {
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

        protected abstract Tuple<Expression, Expression> GetSerializerExp(Expression valueExp, Expression codec);

        protected abstract Tuple<Expression, Expression> GetDeSerializerExp(Expression codec);

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
    }
}