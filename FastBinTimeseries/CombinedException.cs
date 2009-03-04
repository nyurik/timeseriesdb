using System;
using System.Runtime.Serialization;

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// This exception is thrown when an error handling code encountered an error that also needs to be reported.
    /// </summary>
    [Serializable]
    public class CombinedException : Exception
    {
        public readonly Exception InnerSecondary;

        protected CombinedException()
        {
        }

        public CombinedException(string message, Exception innerPrimary, Exception innerSecondary)
            : base(message, innerPrimary)
        {
            if (innerPrimary == null) throw new ArgumentNullException("innerPrimary");
            if (innerSecondary == null) throw new ArgumentNullException("innerSecondary");
            InnerSecondary = innerSecondary;
        }

        protected CombinedException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            InnerSecondary = (Exception) info.GetValue("InnerSecondary", typeof (Exception));
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("InnerSecondary", InnerException, typeof (Exception));
        }
    }
}