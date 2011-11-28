using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    internal class UtcDateTimeSerializer : BaseSerializer
    {
        private readonly MultipliedDeltaSerializer _deltaSerializer;

        /// <summary>
        /// Integer and Float delta serializer.
        /// </summary>
        /// <param name="name">Name of the value (for debugging)</param>
        public UtcDateTimeSerializer(string name)
            : base(typeof (UtcDateTime), name)
        {
            _deltaSerializer = new MultipliedDeltaSerializer(typeof (long), name);
        }

        /// <summary>Value is divided by this parameter before storage</summary>
        public TimeSpan TimeDivider
        {
            get { return TimeSpan.FromTicks(_deltaSerializer.Divider); }
            set { _deltaSerializer.Divider = ValidateDivider(value); }
        }

        private static long ValidateDivider(TimeSpan value)
        {
            if (value < TimeSpan.Zero)
                throw new SerializerException(
                    "Divider ({0}) must be positive", value);
            if (value > TimeSpan.FromDays(1))
                throw new SerializerException("Divider {0} is > 1 day", value);
            if (value > TimeSpan.Zero && TimeSpan.TicksPerDay%value.Ticks != 0)
                throw new SerializerException(
                    "TimeSpan.TicksPerDay must be divisible by time slice {0}", value);
            return value == TimeSpan.Zero ? 1 : value.Ticks;
        }

        protected override Expression GetSerializerExp(Expression valueExp, Expression codec,
                                                       List<ParameterExpression> stateVariables,
                                                       List<Expression> initBlock)
        {
            return _deltaSerializer.GetSerializer(
                Expression.Property(valueExp, "Ticks"), codec, stateVariables, initBlock);
        }

        protected override void GetDeSerializerExp(Expression codec, List<ParameterExpression> stateVariables,
                                                   out Expression readInitValue, out Expression readNextValue)
        {
            Expression readInit, readNext;
            _deltaSerializer.GetDeSerializer(codec, stateVariables, out readInit, out readNext);

            ConstructorInfo ctor = typeof (UtcDateTime).GetConstructor(new[] {typeof (long)});
            if (ctor == null)
                throw new SerializerException("UtcDateTime(long) constructor was not found");
            readInitValue = Expression.New(ctor, readInit);
            readNextValue = Expression.New(ctor, readNext);
        }
    }
}