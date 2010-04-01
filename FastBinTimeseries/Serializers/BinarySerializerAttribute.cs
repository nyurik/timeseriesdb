using System;
using System.Linq;

namespace NYurik.FastBinTimeseries.Serializers
{
    /// <summary>
    /// Use this attribute to specify custom <see cref="IBinSerializer"/> for this type
    /// </summary>
    [AttributeUsage(AttributeTargets.Struct | AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public sealed class BinarySerializerAttribute : Attribute
    {
        private readonly Type _binSerializerType;
        private readonly Type _itemType;

        public BinarySerializerAttribute(Type binSerializerType)
        {
            if (binSerializerType == null)
                throw new ArgumentNullException("binSerializerType");

            _itemType = (from i in binSerializerType.GetInterfaces()
                         where i.IsGenericType && i.GetGenericTypeDefinition() == typeof (IBinSerializer<>)
                         select i.GetGenericArguments()[0]).FirstOrDefault();

            if (_itemType == null)
                throw new ArgumentOutOfRangeException("binSerializerType", binSerializerType,
                                                      "Type does not implement IBinSerializer<T>");

            _binSerializerType = binSerializerType;
        }

        public Type BinSerializerType
        {
            get { return _binSerializerType; }
        }

        public Type ItemType
        {
            get { return _itemType; }
        }
    }
}