using System;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    [AttributeUsage(AttributeTargets.Field, Inherited = false, AllowMultiple = false)]
    public sealed class FieldAttribute : Attribute
    {
        public FieldAttribute()
        {
        }

        public FieldAttribute(Type serializer)
        {
            Serializer = serializer;
        }

        public Type Serializer { get; private set; }
    }
}