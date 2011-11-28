using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    internal class MultipliedDeltaSerializer : BaseSerializer
    {
        private long _divider = 1;
        private ConstantExpression _dividerExp;
        private bool _isInteger;
        private long _multiplier;

        /// <summary>
        /// Integer and Float delta serializer.
        /// </summary>
        /// <param name="valueType">Type of value to store</param>
        /// <param name="name">Name of the value (for debugging)</param>
        public MultipliedDeltaSerializer([NotNull] Type valueType, string name)
            : base(valueType, name)
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
            }
        }

        public override void Validate()
        {
            if (_multiplier < 1)
                throw new SerializerException(
                    "Multiplier = {2} for value {0} ({1}), but must be >= 1", Name, ValueType.FullName, _multiplier);
            if (_divider < 1)
                throw new SerializerException(
                    "Divider = {2} for value {0} ({1}), but must be >= 1", Name, ValueType.FullName, _divider);

            ulong maxDivider = 0;
            _isInteger = true;
            switch (Type.GetTypeCode(ValueType))
            {
                case TypeCode.Char:
                    maxDivider = char.MaxValue;
                    _dividerExp = Expression.Constant((char) _divider);
                    break;
                case TypeCode.Int16:
                    maxDivider = (ulong) Int16.MaxValue;
                    _dividerExp = Expression.Constant((short) _divider);
                    break;
                case TypeCode.UInt16:
                    maxDivider = UInt16.MaxValue;
                    _dividerExp = Expression.Constant((ushort) _divider);
                    break;
                case TypeCode.Int32:
                    maxDivider = Int32.MaxValue;
                    _dividerExp = Expression.Constant((int) _divider);
                    break;
                case TypeCode.UInt32:
                    maxDivider = UInt32.MaxValue;
                    _dividerExp = Expression.Constant((uint) _divider);
                    break;
                case TypeCode.Int64:
                    maxDivider = Int64.MaxValue;
                    // ReSharper disable RedundantCast
                    _dividerExp = Expression.Constant((long) _divider);
                    // ReSharper restore RedundantCast
                    break;
                case TypeCode.UInt64:
                    maxDivider = UInt64.MaxValue;
                    _dividerExp = Expression.Constant((ulong) _divider);
                    break;
                case TypeCode.Single:
                case TypeCode.Double:
                    _isInteger = false;
                    break;
                default:
                    throw new SerializerException(
                        "Value {0} has an unsupported type {0}", Name, ValueType.AssemblyQualifiedName);
            }

            if (_isInteger)
            {
                if (_multiplier != 1)
                    throw new SerializerException(
                        "Integer types must have multiplier == 1, but {0} was given instead for value {0} ({1})",
                        _multiplier, Name, ValueType.FullName);
                if ((ulong) _divider > maxDivider)
                    throw new SerializerException(
                        "Divider = {2} for value {0} ({1}), but must be < {3}", Name, ValueType.FullName, _divider,
                        maxDivider);
            }

            base.Validate();
        }

        protected override Expression GetSerializerExp(Expression valueExp, Expression codec,
                                                       List<ParameterExpression> stateVariables,
                                                       List<Expression> initBlock)
        {
            ThrowOnNotInitialized();

            //
            // long stateVar;
            //
            ParameterExpression stateVarExp = Expression.Variable(typeof (long), "state" + Name);
            stateVariables.Add(stateVarExp);

            //
            // valueGetter():
            //    if non-integer: (#long) Math.Round(value * Multiplier / Divider, 0)
            //    if integer:     (#long) (value / divider)
            //

            Expression getValExp = valueExp;

            if (_multiplier != 1 || _divider != 1)
            {
                if (!_isInteger)
                {
                    switch (Type.GetTypeCode(ValueType))
                    {
                        case TypeCode.Single:
                            {
                                // floats support 7 significant digits
                                float dvdr = (float) _multiplier/_divider;
                                float maxValue = (float) Math.Pow(10, 7)/dvdr;

                                getValExp = FloatingGetValExp(getValExp, codec, dvdr, -maxValue, maxValue);
                            }
                            break;

                        case TypeCode.Double:
                            {
                                // doubles support at least 15 significant digits
                                double dvdr = (double) _multiplier/_divider;
                                double maxValue = Math.Pow(10, 15)/dvdr;

                                getValExp = FloatingGetValExp(getValExp, codec, dvdr, -maxValue, maxValue);
                            }
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
                else
                {
                    if (_multiplier != 1) throw new InvalidOperationException();
                    getValExp = Expression.Divide(getValExp, _dividerExp);
                }
            }

            // for integer types, do not check, ulong would not fit otherwise
            if (getValExp.Type != typeof (long))
                getValExp =
                    _isInteger
                        ? Expression.Convert(getValExp, typeof (long))
                        : Expression.ConvertChecked(getValExp, typeof (long));


            //
            // stateVar = valueGetter();
            // codec.WriteSignedValue(stateVar);
            //
            initBlock.Add(Expression.Assign(stateVarExp, getValExp));
            initBlock.Add(DebugLong(codec, stateVarExp));
            initBlock.Add(WriteSignedValue(codec, stateVarExp));

            //
            // stateVar2 = valueGetter();
            // delta = stateVar2 - stateVar
            // stateVar = stateVar2;
            // return codec.WriteSignedValue(delta);
            //
            ParameterExpression stateVar2Exp = Expression.Variable(typeof (long), "state2" + Name);
            ParameterExpression deltaExp = Expression.Variable(typeof (long), "delta" + Name);
            return
                Expression.Block(
                    typeof (bool),
                    new[] {stateVar2Exp, deltaExp},
                    Expression.Assign(stateVar2Exp, getValExp),
                    Expression.Assign(deltaExp, Expression.Subtract(stateVar2Exp, stateVarExp)),
                    Expression.Assign(stateVarExp, stateVar2Exp),
                    DebugLong(codec, stateVarExp),
                    WriteSignedValue(codec, deltaExp)
                    );
        }

        private Expression FloatingGetValExp<T>(Expression value, Expression codec, T divider,
                                                T minValue, T maxValue)
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
                        ThrowOverflow(codec, value)),
                    // Math.Round(value*_multiplier/_divider)
                    Expression.Call(typeof (Math), "Round", null, multExp));
        }

        protected override void GetDeSerializerExp(Expression codec, List<ParameterExpression> stateVariables,
                                                   out Expression readInitValue, out Expression readNextValue)
        {
            ThrowOnNotInitialized();

            //
            // long stateVar;
            //
            ParameterExpression stateVarExp = Expression.Variable(typeof (long), "state" + Name);
            stateVariables.Add(stateVarExp);

            //
            // valueGetter():
            //    if non-integer: (decimal)value * ((decimal)Divider / Multiplier)
            //    if integer:     (type)value * divider
            //

            Expression getValExp = stateVarExp;

            if (_multiplier != 1 || _divider != 1)
            {
                if (!_isInteger)
                {
                    switch (Type.GetTypeCode(ValueType))
                    {
                        case TypeCode.Single:
                            getValExp = Expression.Divide(
                                Expression.Convert(getValExp, typeof (float)),
                                Expression.Constant((float) _multiplier/_divider));
                            break;

                        case TypeCode.Double:
                            getValExp = Expression.Divide(
                                Expression.Convert(getValExp, typeof (double)),
                                Expression.Constant((double) _multiplier/_divider));
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
                else
                {
                    if (_multiplier != 1) throw new InvalidOperationException();
                    if (getValExp.Type != ValueType)
                        getValExp = Expression.Convert(getValExp, ValueType);
                    getValExp = Expression.Multiply(getValExp, _dividerExp);
                }
            }
            else if (getValExp.Type != ValueType)
                getValExp = Expression.Convert(getValExp, ValueType);


            //
            // stateVar = codec.ReadSignedValue();
            // return stateVar;
            //
            MethodCallExpression readValExp = ReadSignedValue(codec);
            readInitValue = Expression.Block(
                Expression.Assign(stateVarExp, readValExp),
                DebugLong(codec, stateVarExp),
                getValExp);

            //
            // stateVar += codec.ReadSignedValue();
            // return stateVar;
            //
            readNextValue = Expression.Block(
                Expression.AddAssign(stateVarExp, readValExp),
                DebugLong(codec, stateVarExp),
                getValExp);
        }
    }
}