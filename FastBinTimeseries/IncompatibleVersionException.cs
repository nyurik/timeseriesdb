using System;
using System.Runtime.Serialization;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    [Serializable]
    public class IncompatibleVersionException : FormattedException
    {
        protected IncompatibleVersionException(SerializationInfo info, StreamingContext context) : base(info, context)
        {}

        public IncompatibleVersionException(Type type, Version version)
            : base("Version {0} is not supported by type {1}", version, type.AssemblyQualifiedName)
        {}
    }
}