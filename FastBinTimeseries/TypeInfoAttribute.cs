using System;
using System.Runtime.InteropServices;

namespace NYurik.FastBinTimeseries
{
    [AttributeUsage(AttributeTargets.Field, Inherited = false, AllowMultiple = false)]
    internal sealed class TypeInfoAttribute : Attribute
    {
        private readonly Type valueType;

        public TypeInfoAttribute(Type valueType)
        {
            this.valueType = valueType;
        }

        public Type ValueType
        {
            get { return valueType; }
        }

        public int GetTypeSize()
        {
            if (valueType == null)
                return 0;
            return valueType == typeof (DateTime) || valueType == typeof (TimeSpan)
                       ? Marshal.SizeOf(typeof (long))
                       : Marshal.SizeOf(valueType);
        }
    }
}