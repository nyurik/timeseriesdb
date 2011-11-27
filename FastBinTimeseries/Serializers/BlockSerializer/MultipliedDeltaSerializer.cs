using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    internal class MultipliedDeltaSerializer : BaseSerializer
    {
        private int _divider;
        private ConstantExpression _dividerExp;
        private bool _isInteger;
        private int _multiplier;

        /// <summary>
        /// Integer and Float delta serializer.
        /// </summary>
        /// <param name="valueType">Type of value to store</param>
        /// <param name="name">Name of the value (for debugging)</param>
        /// <param name="multiplier">Value is multiplied by this parameter before storage</param>
        /// <param name="divider">Value is divided by this parameter before storage</param>
        public MultipliedDeltaSerializer([NotNull] Type valueType, string name, int multiplier = 1, int divider = 1)
            : base(valueType, name)
        {
            _multiplier = multiplier;
            _divider = divider;
        }

        public int Multiplier
        {
            get { return _multiplier; }
            set
            {
                ThrowOnInitialized();
                _multiplier = value;
            }
        }

        public int Divider
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
                    _dividerExp = Expression.Constant(_divider);
                    break;
                case TypeCode.UInt32:
                    maxDivider = UInt32.MaxValue;
                    _dividerExp = Expression.Constant((uint) _divider);
                    break;
                case TypeCode.Int64:
                    maxDivider = Int64.MaxValue;
                    _dividerExp = Expression.Constant((long) _divider);
                    break;
                case TypeCode.UInt64:
                    maxDivider = UInt64.MaxValue;
                    _dividerExp = Expression.Constant((ulong) _divider);
                    break;
                case TypeCode.Single:
                case TypeCode.Double:
                case TypeCode.Decimal:
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
            ParameterExpression stateVarExp = Expression.Variable(typeof (long), "state_" + Name);
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
                    getValExp =
                        Expression.Call(
                            typeof (Math), "Round", null,
                            Expression.Multiply(
                                Expression.Convert(getValExp, typeof (decimal)),
                                Expression.Constant((decimal) _multiplier/_divider)),
                            Expression.Constant(0));
                else
                {
                    if (_multiplier != 1) throw new InvalidOperationException();
                    getValExp = Expression.Divide(getValExp, _dividerExp);
                }
            }

            if (getValExp.Type != typeof (long))
                getValExp = _isInteger
                                ? Expression.Convert(getValExp, typeof (long))
                                : Expression.ConvertChecked(getValExp, typeof (long));


            //
            // stateVar = valueGetter();
            // codec.WriteSignedValue(stateVar);
            //
            initBlock.Add(Expression.Assign(stateVarExp, getValExp));
            //initBlock.Add(DebugLong(codec, stateVarExp));
            initBlock.Add(WriteSignedValue(codec, stateVarExp));

            //
            // stateVar2 = valueGetter();
            // delta = stateVar2 - stateVar
            // stateVar = stateVar2;
            // return codec.WriteSignedValue(delta);
            //
            ParameterExpression stateVar2Exp = Expression.Variable(typeof (long), "state2_" + Name);
            ParameterExpression deltaExp = Expression.Variable(typeof (long), "delta_" + Name);
            return
                Expression.Block(
                    typeof (bool),
                    new[] {stateVar2Exp, deltaExp},
                    Expression.Assign(stateVar2Exp, getValExp),
                    Expression.Assign(deltaExp, Expression.Subtract(stateVar2Exp, stateVarExp)),
                    Expression.Assign(stateVarExp, stateVar2Exp),
                    //DebugLong(codec, stateVarExp),
                    WriteSignedValue(codec, deltaExp)
                    );
        }

        protected override void GetDeSerializerExp(Expression valueExp, Expression codec,
                                                   List<ParameterExpression> stateVariables,
                                                   List<Expression> initBlock, List<Expression> deltaBlock)
        {
            ThrowOnNotInitialized();

            //
            // long stateVar;
            //
            ParameterExpression stateVarExp = Expression.Variable(typeof (long), "state_" + Name);
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
                    getValExp =
                        Expression.Multiply(
                            Expression.Convert(getValExp, typeof (decimal)),
                            Expression.Constant((decimal) _divider/_multiplier));
                    if (getValExp.Type != valueExp.Type)
                        getValExp = Expression.ConvertChecked(getValExp, valueExp.Type);
                }
                else
                {
                    if (_multiplier != 1) throw new InvalidOperationException();
                    if (getValExp.Type != valueExp.Type)
                        getValExp = Expression.Convert(getValExp, valueExp.Type);
                    getValExp = Expression.Multiply(getValExp, _dividerExp);
                }
            }
            else if (getValExp.Type != valueExp.Type)
                getValExp = Expression.Convert(getValExp, valueExp.Type);


            // expression: T.Field = valueGetter()
            BinaryExpression setFieldExp = Expression.Assign(valueExp, getValExp);


            //
            // stateVar = codec.ReadSignedValue();
            // T.Field = stateVar;
            //
            MethodCallExpression readValExp = ReadSignedValue(codec);
            initBlock.Add(Expression.Assign(stateVarExp, readValExp));
            //initBlock.Add(DebugLong(codec, stateVarExp)); 
            initBlock.Add(setFieldExp);

            //
            // stateVar += codec.ReadSignedValue();
            // T.Field = stateVar;
            //
            deltaBlock.Add(Expression.AddAssign(stateVarExp, readValExp));
            //deltaBlock.Add(DebugLong(codec, stateVarExp));
            deltaBlock.Add(setFieldExp);
        }
    }
}