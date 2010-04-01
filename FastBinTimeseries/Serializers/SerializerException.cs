using System;
using System.Runtime.Serialization;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries.Serializers
{
    [Serializable]
    public class SerializerException : FormattedException
    {
        public SerializerException()
        {}

        public SerializerException(string message) : base(message)
        {}

        public SerializerException(string message, params object[] args) : base(message, args)
        {}

        public SerializerException(Exception inner, string message) : base(inner, message)
        {}

        public SerializerException(Exception inner, string message, params object[] args) : base(inner, message, args)
        {}

        protected SerializerException(SerializationInfo info, StreamingContext context) : base(info, context)
        {}
    }
}