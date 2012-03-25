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
using System.IO;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.TimeSeriesDb.Serializers.BlockSerializer
{
    public class ScaledDeltaField : BaseField
    {
        private DeltaType _deltaType;
        private long _divider = 1;
        private ConstantExpression _dividerExp;
        private bool _isInteger;
        private long _multiplier;
        private double _precision = double.NaN;

        [UsedImplicitly]
        protected ScaledDeltaField()
        {
        }

        /// <summary>
        /// Integer and Float delta serializer.
        /// </summary>
        /// <param name="stateStore">Serializer with the state</param>
        /// <param name="valueType">Type of value to store</param>
        /// <param name="stateName">Name of the value (for debugging)</param>
        public ScaledDeltaField([NotNull] IStateStore stateStore, [NotNull] Type valueType, string stateName)
            : base(Version11, stateStore, valueType, stateName)
        {
            // Floating point numbers must manually initialize Multiplier
            _multiplier =
                valueType.IsPrimitive && (valueType == typeof (float) || valueType == typeof (double))
                    ? 0
                    : 1;
        }

        /// <summary> Value is multiplied by this parameter before storage</summary>
        public long Multiplier
        {
            get { return _multiplier; }
            set
            {
                ThrowOnInitialized();
                _multiplier = value;
                UpdatePrecision();
            }
        }

        /// <summary> Value is divided by this parameter before storage </summary>
        public long Divider
        {
            get { return _divider; }
            set
            {
                ThrowOnInitialized();
                _divider = value;
                UpdatePrecision();
            }
        }

        /// <summary> If not NaN, the new data will be checked for loss of precision before storing.
        /// By default, the precision is set to 1/100th of the original value resolution whenever Multiplier or Divider
        /// are set by the user. If the Divider is 1, and Multiplier is a 100 (storing two decimal places),
        /// 5.24001 will be stored as 5.24, but 5.2401 will cause an error.
        /// 
        /// This error check will be performed when serializing:
        /// if (Math.Abs(Math.Round(value * Multiplier / Divider, 0) * ((T)Divider / Multiplier) - value) > Precision)
        ///     throw new SerializerException();
        /// 
        /// To override the default, make sure it is set to a other value or NaN after initializing Multiplier and Divider.
        /// </summary>
        public double Precision
        {
            get { return _precision; }
            set
            {
                ThrowOnInitialized();
                if (!double.IsNaN(value) && Version < Version11)
                    throw new ArgumentOutOfRangeException(
                        "value", value, "value may not be non-NaN when field version is less than 1.1");
                if (double.IsInfinity(value))
                    throw new ArgumentOutOfRangeException("value", value, "value may not be infinity");
                _precision = value;
            }
        }

        /// <summary> When the field value can only increase or only decrease, set this value to store deltas as unsigned integer.
        /// This results in some storage space gains - for instance a delta between 64 and 127 will now need 1 byte instead of 2.
        /// </summary>
        public DeltaType DeltaType
        {
            get { return _deltaType; }
            set
            {
                ThrowOnInitialized();
                if (!Enum.IsDefined(typeof (DeltaType), value))
                    throw new ArgumentOutOfRangeException("value", value, "This value is not defined in DeltaType enum");
                _deltaType = value;
            }
        }

        public override Version Version
        {
            get { return base.Version; }
            set
            {
                base.Version = value;
                UpdatePrecision();
            }
        }

        public override int GetMaxByteSize()
        {
            // TODO: optimize to make this number smaller depending on the field type and scaling parameters
            return CodecBase.MaxBytesFor64;
        }

        protected override void InitNewField(BinaryWriter writer)
        {
            base.InitNewField(writer);

            writer.Write(Divider);
            writer.Write(Multiplier);

            if (Version >= Version11)
            {
                writer.Write((byte) DeltaType);
                writer.Write(BitConverter.DoubleToInt64Bits(Precision));
            }
        }

        protected override void InitExistingField(BinaryReader reader, Func<string, Type> typeResolver)
        {
            base.InitExistingField(reader, typeResolver);

            Divider = reader.ReadInt64();
            Multiplier = reader.ReadInt64();

            if (Version >= Version11)
            {
                DeltaType = (DeltaType) reader.ReadByte();
                Precision = BitConverter.Int64BitsToDouble(reader.ReadInt64());
            }
        }

        protected override bool IsValidVersion(Version ver)
        {
            return ver == Version10 || ver == Version11;
        }

        protected override void MakeReadonly()
        {
            if (Multiplier < 1)
                throw new SerializerException(
                    "Multiplier = {0} for value {1} ({2}), but must be >= 1", Multiplier, StateName, ValueType.FullName);
            if (Divider < 1)
                throw new SerializerException(
                    "Divider = {0} for value {1} ({2}), but must be >= 1", Divider, StateName, ValueType.FullName);

            ulong maxDivider = 0;
            _isInteger = true;
            switch (ValueTypeCode)
            {
                case TypeCode.Char:
                    maxDivider = char.MaxValue;
                    _dividerExp = Expression.Constant((char) Divider);
                    break;
                case TypeCode.Int16:
                    maxDivider = (ulong) Int16.MaxValue;
                    _dividerExp = Expression.Constant((short) Divider);
                    break;
                case TypeCode.UInt16:
                    maxDivider = UInt16.MaxValue;
                    _dividerExp = Expression.Constant((ushort) Divider);
                    break;
                case TypeCode.Int32:
                    maxDivider = Int32.MaxValue;
                    _dividerExp = Expression.Constant((int) Divider);
                    break;
                case TypeCode.UInt32:
                    maxDivider = UInt32.MaxValue;
                    _dividerExp = Expression.Constant((uint) Divider);
                    break;
                case TypeCode.Int64:
                    maxDivider = Int64.MaxValue;
                    // ReSharper disable RedundantCast
                    _dividerExp = Expression.Constant((long) Divider);
                    // ReSharper restore RedundantCast
                    break;
                case TypeCode.UInt64:
                    maxDivider = UInt64.MaxValue;
                    _dividerExp = Expression.Constant((ulong) Divider);
                    break;
                case TypeCode.Single:
                case TypeCode.Double:
                    _isInteger = false;
                    break;
                default:
                    throw new SerializerException(
                        "Value {0} has an unsupported type {1}", StateName, ValueType.AssemblyQualifiedName);
            }

            if (_isInteger)
            {
                if (Multiplier != 1)
                    throw new SerializerException(
                        "Integer types must have multiplier == 1, but {0} was given instead for value {1} ({2})",
                        Multiplier, StateName, ValueType.FullName);
                if ((ulong) Divider > maxDivider)
                    throw new SerializerException(
                        "Divider = {0} for value {1} ({2}), but must be < {3}", Divider, StateName, ValueType.FullName,
                        maxDivider);
            }

            base.MakeReadonly();
        }

        protected override Tuple<Expression, Expression> GetSerializerExp(Expression valueExp, Expression codec)
        {
            //
            // long stateVar;
            //
            bool needToInit;
            ParameterExpression stateVarExp = StateStore.GetOrCreateStateVar(StateName, typeof (long), out needToInit);

            //
            // valueGetter():
            //    if non-integer: (#long) Math.Round(value * Multiplier / Divider, 0)
            //    if integer:     (#long) (value / divider)
            //

            Expression getValExp = valueExp;

            if (Multiplier != 1 || Divider != 1)
            {
                if (!_isInteger)
                {
                    switch (ValueTypeCode)
                    {
                        case TypeCode.Single:
                            {
                                // floats support 7 significant digits
                                float dvdr = (float) Multiplier/Divider;
                                float maxValue = (float) Math.Pow(10, 7)/dvdr;
                                getValExp = FloatingGetValExp(getValExp, dvdr, -maxValue, maxValue);
                            }
                            break;

                        case TypeCode.Double:
                            {
                                // doubles support at least 15 significant digits
                                double dvdr = (double) Multiplier/Divider;
                                double maxValue = Math.Pow(10, 15)/dvdr;
                                getValExp = FloatingGetValExp(getValExp, dvdr, -maxValue, maxValue);
                            }
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
                else
                {
                    if (Multiplier != 1) throw new InvalidOperationException();
                    getValExp = Expression.Divide(getValExp, _dividerExp);
                }
            }

            // for integer types, do not check, ulong would not fit otherwise
            if (getValExp.Type != typeof (long))
                getValExp =
                    _isInteger
                        ? Expression.Convert(getValExp, typeof (long))
                        : Expression.ConvertChecked(getValExp, typeof (long));


            ParameterExpression varState2Exp = Expression.Variable(typeof (long), "state2");
            ParameterExpression varDeltaExp = Expression.Variable(typeof (long), "delta");

            //
            // stateVar2 = valueGetter();
            // delta = stateVar2 - stateVar
            //
            var exprs =
                new List<Expression>
                    {
                        Expression.Assign(varState2Exp, getValExp),
                        Expression.Assign(varDeltaExp, Expression.Subtract(varState2Exp, stateVarExp))
                    };

            //
            // DeltaType.Positive: if (delta < 0) throw SerializerException();
            // DeltaType.Negative: if (delta > 0) throw SerializerException();
            //
            if (DeltaType == DeltaType.Positive)
                exprs.Add(
                    Expression.IfThen(
                        Expression.LessThan(varDeltaExp, Expression.Constant((long) 0)),
                        ThrowSerializer(
                            Expression.Constant("Value {0} is smaller than previous value in a positive delta field"),
                            varState2Exp)));
            else if (DeltaType == DeltaType.Negative)
                exprs.Add(
                    Expression.IfThen(
                        Expression.IsFalse(Expression.LessThanOrEqual(varDeltaExp, Expression.Constant((long) 0))),
                        ThrowSerializer(
                            Expression.Constant("Value {0} is larger than previous value in a negative delta field"),
                            varState2Exp)));

            //
            // stateVar = stateVar2;
            // DEBUG: DebugValue(stateVar);
            //
            exprs.Add(Expression.Assign(stateVarExp, varState2Exp));
            exprs.Add(DebugValueExp(codec, stateVarExp, "MultFld WriteDelta"));

            //
            // DeltaType.Signed: return codec.WriteSignedValue(delta);
            // DeltaType.Positive: return codec.WriteUnsignedValue(delta);
            // DeltaType.Negative: return codec.WriteUnsignedValue(-delta);
            //
            switch (DeltaType)
            {
                case DeltaType.Signed:
                    exprs.Add(WriteSignedValue(codec, varDeltaExp));
                    break;
                case DeltaType.Positive:
                    exprs.Add(WriteUnsignedValue(codec, Expression.Convert(varDeltaExp, typeof (ulong))));
                    break;
                case DeltaType.Negative:
                    exprs.Add(
                        WriteUnsignedValue(codec, Expression.Convert(Expression.Negate(varDeltaExp), typeof (ulong))));
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            Expression deltaExp = Expression.Block(typeof (bool), new[] {varState2Exp, varDeltaExp}, exprs);

            //
            // stateVar = valueGetter();
            // codec.WriteSignedValue(stateVar);
            //
            Expression initExp =
                needToInit
                    ? Expression.Block(
                        Expression.Assign(stateVarExp, getValExp),
                        DebugValueExp(codec, stateVarExp, "MultFld WriteInit"),
                        // The first item is always stored as a signed long
                        WriteSignedValue(codec, stateVarExp))
                    : deltaExp;

            return new Tuple<Expression, Expression>(initExp, deltaExp);
        }

        private Expression FloatingGetValExp<T>(
            Expression value, T divider, T minValue, T maxValue)
        {
            Expression multExp = Expression.Multiply(value, Expression.Constant(divider));
            if (value.Type == typeof (float))
                multExp = Expression.Convert(multExp, typeof (double));

            return
                Expression.Block(
                    // if (value < -maxValue || maxValue < value)
                    //     ThrowOverflow(value);
                    Expression.IfThen(
                        Expression.Or(
                            Expression.LessThan(value, Expression.Constant(minValue)),
                            Expression.LessThan(Expression.Constant(maxValue), value)),
                        ThrowOverflow(value)),
                    // Math.Round(value*Multiplier/Divider)
                    Expression.Call(typeof (Math), "Round", null, multExp));
        }

        protected override Tuple<Expression, Expression> GetDeSerializerExp(Expression codec)
        {
            //
            // long stateVar;
            //
            bool needToInit;
            ParameterExpression stateVarExp = StateStore.GetOrCreateStateVar(StateName, typeof (long), out needToInit);

            //
            // valueGetter():
            //    if non-integer: (T)value * ((T)Divider / Multiplier)
            //    if integer:     (T)value * divider
            //

            Expression getValExp = stateVarExp;

            if (Multiplier != 1 || Divider != 1)
            {
                if (!_isInteger)
                {
                    switch (ValueTypeCode)
                    {
                        case TypeCode.Single:
                            getValExp = Expression.Divide(
                                Expression.Convert(getValExp, typeof (float)),
                                Expression.Constant((float) Multiplier/Divider));
                            break;

                        case TypeCode.Double:
                            getValExp = Expression.Divide(
                                Expression.Convert(getValExp, typeof (double)),
                                Expression.Constant((double) Multiplier/Divider));
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
                else
                {
                    if (Multiplier != 1) throw new InvalidOperationException();
                    if (getValExp.Type != ValueType)
                        getValExp = Expression.Convert(getValExp, ValueType);
                    getValExp = Expression.Multiply(getValExp, _dividerExp);
                }
            }
            else if (getValExp.Type != ValueType)
                getValExp = Expression.Convert(getValExp, ValueType);

            // How to read value - depending on delta type
            Expression readValExp;
            switch (DeltaType)
            {
                case DeltaType.Signed:
                    readValExp = ReadSignedValue(codec);
                    break;
                case DeltaType.Positive:
                    readValExp = Expression.Convert(ReadUnsignedValue(codec), typeof (long));
                    break;
                case DeltaType.Negative:
                    readValExp = Expression.Negate(Expression.Convert(ReadUnsignedValue(codec), typeof (long)));
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            //
            // stateVar += codec.ReadSignedValue();
            // return stateVar;
            //
            Expression deltaExp =
                Expression.Block(
                    Expression.AddAssign(stateVarExp, readValExp),
                    DebugValueExp(codec, stateVarExp, "MultFld ReadDelta"),
                    getValExp);

            //
            // stateVar = codec.ReadSignedValue();
            // return stateVar;
            //
            Expression initExp =
                needToInit
                    ? Expression.Block(
                        Expression.Assign(stateVarExp, readValExp),
                        DebugValueExp(codec, stateVarExp, "MultFld ReadInit"),
                        getValExp)
                    : deltaExp;

            return new Tuple<Expression, Expression>(initExp, deltaExp);
        }

        private void UpdatePrecision()
        {
            Precision = Version > Version10 && Multiplier != 0 && Divider != 0
                            ? (double) Divider/Multiplier/100.0
                            : Double.NaN;
        }
    }

    public enum DeltaType : byte
    {
        Signed,
        Positive,
        Negative,
    }
}