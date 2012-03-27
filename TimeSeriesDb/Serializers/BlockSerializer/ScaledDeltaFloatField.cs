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
using System.IO;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.TimeSeriesDb.Serializers.BlockSerializer
{
    public class ScaledDeltaFloatField : ScaledDeltaBaseField
    {
        private long _divider = 1;
        private long _multiplier;
        private double _precision = double.NaN;

        [UsedImplicitly]
        protected ScaledDeltaFloatField()
        {
        }

        /// <summary>
        /// Integer and Float delta serializer.
        /// </summary>
        /// <param name="stateStore">Serializer with the state</param>
        /// <param name="valueType">Type of value to store</param>
        /// <param name="stateName">Name of the value (for debugging)</param>
        public ScaledDeltaFloatField([NotNull] IStateStore stateStore, [NotNull] Type valueType, string stateName)
            : base(Version10, stateStore, valueType, stateName)
        {
            // Floating point numbers must manually initialize Multiplier
            _multiplier = 0;
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
                if (double.IsInfinity(value))
                    throw new ArgumentOutOfRangeException("value", value, "value may not be infinity");
                if (value < 0)
                    throw new ArgumentOutOfRangeException("value", value, "value may not be negative");
                _precision = value;
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

        protected override void InitNewField(BinaryWriter writer)
        {
            base.InitNewField(writer);

            writer.Write(BitConverter.DoubleToInt64Bits(Precision));
            writer.Write(Divider);
            writer.Write(Multiplier);
        }

        protected override void InitExistingField(BinaryReader reader, Func<string, Type> typeResolver)
        {
            base.InitExistingField(reader, typeResolver);

            Precision = BitConverter.Int64BitsToDouble(reader.ReadInt64());
            Divider = reader.ReadInt64();
            Multiplier = reader.ReadInt64();
        }

        protected override bool IsValidVersion(Version ver)
        {
            return ver == Version10;
        }

        protected override void MakeReadonly()
        {
            if (Multiplier < 1)
                throw new SerializerException(
                    "Multiplier = {0} for value {1} ({2}), but must be >= 1", Multiplier, StateName, ValueType.FullName);
            if (Divider < 1)
                throw new SerializerException(
                    "Divider = {0} for value {1} ({2}), but must be >= 1", Divider, StateName, ValueType.FullName);

            base.MakeReadonly();
        }

        protected override Expression OnWriteValidation(Expression codec, Expression valueExp, ParameterExpression stateVarExp)
        {
            // if (Precision < Math.Abs(valueFromState - originalValue))
            //     throw new SerializerException();
            return
                double.IsNaN(Precision)
                    ? null
                    : Expression.IfThen(
                        Expression.LessThan(
                            Const(Precision, ValueType),
                            Expression.Call(
                                typeof(Math), "Abs", null,
                                Expression.Subtract(
                                    StateToValue(stateVarExp),
                                    valueExp))),
                        ThrowSerializer(
                            codec,
                            Const("Value {0} would loose precision when stored"),
                            valueExp));
        }

        protected override Expression ValueToState(Expression codec, Expression valueExp)
        {
            //
            // valueGetter(): (#long) Math.Round(value * Multiplier / Divider, 0)
            //
            Expression getValExp;

            if (Multiplier != 1 || Divider != 1)
            {
                switch (ValueTypeCode)
                {
                    case TypeCode.Single:
                        {
                            // floats support 7 significant digits
                            float scale = (float) Multiplier/Divider;
                            float maxValue = (float) Math.Pow(10, 7)/scale;
                            getValExp = FloatingGetValExp(codec, valueExp, scale, -maxValue, maxValue);
                        }
                        break;

                    case TypeCode.Double:
                        {
                            // doubles support at least 15 significant digits
                            double scale = (double) Multiplier/Divider;
                            double maxValue = Math.Pow(10, 15)/scale;
                            getValExp = FloatingGetValExp(codec, valueExp, scale, -maxValue, maxValue);
                        }
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            else
            {
                getValExp = valueExp;
            }

            return Expression.ConvertChecked(getValExp, typeof (long));
        }

        private Expression FloatingGetValExp<T>(Expression codec, Expression value, T scale, T minValue, T maxValue)
        {
            Expression multExp = Expression.Multiply(value, Const(scale));
            if (value.Type == typeof (float))
                multExp = Expression.Convert(multExp, typeof (double));

            return
                Expression.Block(
                    // if (value < -maxValue || maxValue < value)
                    //     ThrowOverflow(value);
                    Expression.IfThen(
                        Expression.Or(
                            Expression.LessThan(value, Const(minValue)),
                            Expression.LessThan(Const(maxValue), value)),
                        ThrowOverflow(codec, value)),
                    // return Math.Round(value*Multiplier/Divider)
                    Expression.Call(typeof (Math), "Round", null, multExp));
        }

        /// <summary>
        /// valueGetter():
        ///    if non-integer: (T)state * ((T)Divider / Multiplier)
        ///    if integer:     (T)state * divider
        /// </summary>
        protected override Expression StateToValue(ParameterExpression stateVarExp)
        {
            Expression getValExp = stateVarExp;

            if (Multiplier != 1 || Divider != 1)
            {
                switch (ValueTypeCode)
                {
                    case TypeCode.Single:
                        getValExp = Expression.Divide(
                            Expression.Convert(getValExp, typeof (float)),
                            Const((float) Multiplier/Divider));
                        break;

                    case TypeCode.Double:
                        getValExp = Expression.Divide(
                            Expression.Convert(getValExp, typeof (double)),
                            Const((double) Multiplier/Divider));
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            else if (getValExp.Type != ValueType)
                getValExp = Expression.Convert(getValExp, ValueType);

            return getValExp;
        }

        private void UpdatePrecision()
        {
            Precision = Multiplier != 0 && Divider != 0
                            ? (double) Divider/Multiplier/1000.0
                            : Double.NaN;
        }
    }
}