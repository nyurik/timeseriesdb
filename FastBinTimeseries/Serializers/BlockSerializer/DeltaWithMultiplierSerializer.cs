using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    internal class DeltaWithMultiplierSerializer : BaseSerializer
    {
        private int _divider;
        private bool _isInteger;
        private int _multiplier;
        private string _name;

        /// <summary>
        /// Integer and Float delta serializer.
        /// </summary>
        /// <param name="valueType">Type of value to store</param>
        /// <param name="name">Name of the value (for debugging)</param>
        /// <param name="multiplier">Value is multiplied by this parameter before storage</param>
        /// <param name="divider">Value is divided by this parameter before storage</param>
        public DeltaWithMultiplierSerializer([NotNull] Type valueType, [NotNull] string name, int multiplier = 1,
                                             int divider = 1)
            :base(valueType)
        {
            _multiplier = multiplier;
            _name = name;
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

        public string Name
        {
            get { return _name; }
            set
            {
                ThrowOnInitialized();
                _name = value;
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
            if (_name == null)
                throw new SerializerException("Name is null for value of type {0}", ValueType.FullName);
            if (_multiplier == 0)
                throw new SerializerException("Multiplier for value {0} ({1}) may not be 0", _name, ValueType.FullName);
            if (_divider == 0)
                throw new SerializerException("Divider for value {0} ({1}) may not be 0", _name, ValueType.FullName);

            switch (Type.GetTypeCode(ValueType))
            {
                case TypeCode.Char:
                case TypeCode.SByte:
                case TypeCode.Byte:
                case TypeCode.Int16:
                case TypeCode.UInt16:
                case TypeCode.Int32:
                case TypeCode.UInt32:
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    _isInteger = true;
                    if (_multiplier != 1)
                        throw new SerializerException(
                            "Integer types must have multiplier == 1, but {0} was given instead for value {0} ({1})",
                            _multiplier, _name, ValueType.FullName);
                    break;
                case TypeCode.Single:
                case TypeCode.Double:
                case TypeCode.Decimal:
                    _isInteger = false;
                    break;
                default:
                    throw new SerializerException(
                        "Value {0} has an unsupported type {0}",
                        _name, ValueType.AssemblyQualifiedName);
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
            ParameterExpression stateVarExp = Expression.Variable(typeof (long), "state_" + _name);
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
                    getValExp = Expression.Divide(getValExp, Expression.Constant(_divider));
                }
            }

            if (getValExp.Type != typeof (long))
                getValExp = Expression.ConvertChecked(getValExp, typeof (long));


            //
            // stateVar = valueGetter();
            // codec.WriteSignedValue(stateVar);
            //
            initBlock.Add(Expression.Assign(stateVarExp, getValExp));
            // ReSharper disable PossiblyMistakenUseOfParamsMethod
            initBlock.Add(Expression.Call(codec, WriteSignedValueMethod, stateVarExp));
            // ReSharper restore PossiblyMistakenUseOfParamsMethod

            //
            // stateVar2 = valueGetter();
            // delta = stateVar2 - stateVar
            // stateVar = stateVar2;
            // return codec.WriteSignedValue(delta);
            //
            ParameterExpression stateVar2Exp = Expression.Variable(typeof (long), "state2_" + _name);
            ParameterExpression deltaExp = Expression.Variable(typeof (long), "delta_" + _name);
            return
                Expression.Block(
                    typeof (bool),
                    new[] {stateVar2Exp, deltaExp},
                    Expression.Assign(stateVar2Exp, getValExp),
                    Expression.Assign(deltaExp, Expression.Subtract(stateVar2Exp, stateVarExp)),
                    Expression.Assign(stateVarExp, stateVar2Exp),
                    // ReSharper disable PossiblyMistakenUseOfParamsMethod
                    Expression.Call(codec, WriteSignedValueMethod, deltaExp)
                    // ReSharper restore PossiblyMistakenUseOfParamsMethod
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
            ParameterExpression stateVarExp = Expression.Variable(typeof (long), "state_" + _name);
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
                            Expression.Convert(getValExp, typeof(decimal)),
                            Expression.Constant((decimal)_divider / _multiplier));
                    if (getValExp.Type != valueExp.Type)
                        getValExp = Expression.ConvertChecked(getValExp, valueExp.Type);
                }
                else
                {
                    if (_multiplier != 1) throw new InvalidOperationException();
                    if (getValExp.Type != valueExp.Type)
                        getValExp = Expression.Convert(getValExp, valueExp.Type);
                    getValExp = Expression.MultiplyChecked(getValExp, Expression.Constant(_divider));
                }
            }


            // expression: T.Field = valueGetter()
            BinaryExpression setFieldExp = Expression.Assign(valueExp, getValExp);


            //
            // stateVar = codec.ReadSignedValue();
            // T.Field = stateVar;
            //
            MethodCallExpression readValExp = Expression.Call(codec, ReadSignedValueMethod);
            initBlock.Add(Expression.Assign(stateVarExp, readValExp));
            initBlock.Add(setFieldExp);

            //
            // stateVar += codec.ReadSignedValue();
            // T.Field = stateVar;
            //
            deltaBlock.Add(Expression.AddAssign(stateVarExp, readValExp));
            deltaBlock.Add(setFieldExp);
        }
    }
}