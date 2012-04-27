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
        /// <param name="fieldType">Type of value to store</param>
        /// <param name="stateName">Name of the value (for debugging)</param>
        public ScaledDeltaFloatField([NotNull] IStateStore stateStore, [NotNull] Type fieldType, string stateName)
            : base(Versions.Ver0, stateStore, fieldType, stateName)
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
                if (value < 1)
                    throw new SerializerException(
                        "Multiplier = {0} for value {1} ({2}), but must be >= 1", value, StateName, FieldType.FullName);
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
                if (value < 1)
                    throw new SerializerException(
                        "Divider = {0} for value {1} ({2}), but must be >= 1", value, StateName, FieldType.FullName);
                _divider = value;
            }
        }

        public double Scale
        {
            get { return (double) Multiplier/Divider; }
        }

        /// <summary> If not NaN, the new data will be checked for loss of precision before storing.
        ///  
        /// If Scale (Multiplier/Divider) is 100 (storing two decimal places), setting Precision to 0.0001
        /// will cause 5.24001 to be stored as 5.24, but 5.2401 will cause an error.
        /// 
        /// Setting precision reduces the range (-max to +max) of numbers that may be stored.
        /// Double has 15 significant digits, while Float - only has 7, so depending on the type,
        ///   float: 10^7 * Precision ==> between -1e3 and +1e3
        ///   double: 10^15 * Precision ==> between -1e11 and +1e11
        /// 
        /// When non NaN, the following data validation will be performed during serialization:
        /// if (Math.Abs(Math.Round(value * Multiplier / Divider, 0) * ((T)Divider / Multiplier) - value) > Precision)
        ///     throw new SerializerException();
        /// 
        /// By default, the precision is set to NaN.
        /// </summary>
        public double Precision
        {
            get { return _precision; }
            set
            {
                ThrowOnInitialized();
                if (double.IsInfinity(value))
                    throw new ArgumentOutOfRangeException("value", value, "Precision may not be infinity");
                if (value <= 0)
                    throw new ArgumentOutOfRangeException("value", value, "Precision must be positive");
                _precision = value;
            }
        }

        private double LargestIntegerValue
        {
            get
            {
                double max;
                switch (FieldType.GetTypeCode())
                {
                    case TypeCode.Single:
                        max = 1e7f; // floats support 7 significant digits
                        break;
                    case TypeCode.Double:
                        max = 1e15d; // doubles support at least 15 significant digits
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
                return max;
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
            return ver == Versions.Ver0;
        }

        protected override void MakeReadonly()
        {
            double minPrec = 10d/LargestIntegerValue/Scale;
            double maxPrec = 1d/Scale;

            if (!double.IsNaN(Precision) && (Precision < minPrec || Precision > maxPrec))
                throw new SerializerException(
                    "Precision = {0} for value {1} ({2}) must be between {3} and {4} (1/Scale)",
                    Precision, StateName, FieldType.FullName, minPrec, maxPrec);

            base.MakeReadonly();
        }

        protected override Expression OnWriteValidation(
            Expression codec, Expression valueExp, ParameterExpression stateVarExp)
        {
            // if (Precision < Math.Abs(valueFromState - originalValue))
            //     throw new SerializerException();
            return
                double.IsNaN(Precision)
                    ? null
                    : Expression.IfThen(
                        Expression.LessThan(
                            Const(Precision, FieldType),
                            Expression.Call(
                                typeof (Math), "Abs", null,
                                Expression.Subtract(
                                    StateToValue(stateVarExp),
                                    valueExp))),
                        ThrowSerializer(
                            codec,
                            "Value {0} would lose precision when stored with multiplier " + Multiplier,
                            valueExp));
        }

        protected override Expression ValueToState(Expression codec, Expression value)
        {
            //
            // if (value < -maxValue || value > maxValue)
            //     ThrowOverflow(value);
            // return (#long) Math.Round(value * Multiplier / Divider)
            //

            double maxConst = LargestIntegerValue*(double.IsNaN(Precision) ? 1/Scale : Precision);
            Expression scaledValue =
                Multiplier != 1 || Divider != 1
                    ? Expression.Multiply(value, Const(Scale, FieldType))
                    : value;

            Expression getValExp = Expression.Block(
                Expression.IfThen(
                    Expression.Or(
                        Expression.LessThan(value, Const(-maxConst, FieldType)),
                        Expression.GreaterThan(value, Const(maxConst, FieldType))),
                    ThrowOverflow(codec, value)),
                Expression.Call(
                    typeof (Math), "Round", null,
                    FieldType == typeof (float)
                        ? Expression.Convert(scaledValue, typeof (double)) // Math.Round(float) does not exist
                        : scaledValue));

            return Expression.ConvertChecked(getValExp, typeof (long));
        }

        /// <summary>
        /// valueGetter(): (T)state * ((T)Divider / Multiplier)
        /// </summary>
        protected override Expression StateToValue(Expression stateVar)
        {
            Expression stateAsT = Expression.Convert(stateVar, FieldType);

            return Multiplier != 1 || Divider != 1
                       ? Expression.Divide(stateAsT, Const(Scale, FieldType))
                       : stateAsT;
        }

        protected override bool Equals(BaseField baseOther)
        {
            var other = (ScaledDeltaFloatField) baseOther;
            return _divider == other._divider && _multiplier == other._multiplier && _precision.Equals(other._precision);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                // ReSharper disable NonReadonlyFieldInGetHashCode
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ _divider.GetHashCode();
                hashCode = (hashCode*397) ^ _multiplier.GetHashCode();
                hashCode = (hashCode*397) ^ _precision.GetHashCode();
                return hashCode;
                // ReSharper restore NonReadonlyFieldInGetHashCode
            }
        }
    }
}