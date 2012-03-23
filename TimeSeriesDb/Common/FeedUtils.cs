using System;

namespace NYurik.TimeSeriesDb.Common
{
    static internal class FeedUtils
    {
        internal static bool IsDefault<TInd>(TInd value)
            where TInd : IComparable<TInd>
        {
            // For value types, it is safe to call IComparable.CompareTo(default) method
            // For refs or interfaces we should only check for null

            // ReSharper disable CompareNonConstrainedGenericWithNull
            return default(TInd) != null
                       ? value.CompareTo(default(TInd)) == 0
                       : value == null;
            // ReSharper restore CompareNonConstrainedGenericWithNull
        }

        public static void AssertPositiveIndex<TInd>(TInd value)
            where TInd : IComparable<TInd>
        {
            if (
                // ReSharper disable CompareNonConstrainedGenericWithNull
                default(TInd) != null
                // ReSharper restore CompareNonConstrainedGenericWithNull
                && value.CompareTo(default(TInd)) < 0)
            {
                throw new BinaryFileException(
                    "Value index '{0}' may not be less than the default '{1}' (negative indexes are not supported",
                    value, default(TInd));
            }
        }

    }
}