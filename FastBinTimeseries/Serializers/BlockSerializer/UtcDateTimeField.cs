using System;
using System.Linq.Expressions;
using System.Reflection;
using JetBrains.Annotations;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    internal class UtcDateTimeField : BaseField
    {
        private readonly MultipliedDeltaField _deltaField;

        /// <summary>
        /// Integer and Float delta serializer.
        /// </summary>
        public UtcDateTimeField([NotNull] IStateStore serializer, string stateName)
            : base(serializer, typeof (UtcDateTime), stateName)
        {
            _deltaField = new MultipliedDeltaField(serializer, typeof (long), stateName);
        }

        /// <summary>Value is divided by this parameter before storage</summary>
        public TimeSpan TimeDivider
        {
            get { return TimeSpan.FromTicks(_deltaField.Divider); }
            set { _deltaField.Divider = ValidateDivider(value); }
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

        protected override Tuple<Expression, Expression> GetSerializerExp(Expression valueExp, Expression codec)
        {
            return _deltaField.GetSerializer(Expression.Property(valueExp, "Ticks"), codec);
        }

        protected override Tuple<Expression, Expression> GetDeSerializerExp(Expression codec)
        {
            Tuple<Expression, Expression> res = _deltaField.GetDeSerializer(codec);

            ConstructorInfo ctor = typeof (UtcDateTime).GetConstructor(new[] {typeof (long)});
            if (ctor == null)
                throw new SerializerException("UtcDateTime(long) constructor was not found");

            return new Tuple<Expression, Expression>(
                Expression.New(ctor, res.Item1),
                Expression.New(ctor, res.Item2));
        }
    }
}