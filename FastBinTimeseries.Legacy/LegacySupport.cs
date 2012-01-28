using System;
using System.Collections.Generic;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public static class LegacySupport
    {
        public static IDictionary<string, Type> GenerateMapping()
        {
            Type[] types = typeof (LegacySupport).Assembly.GetTypes();
            var dict = new Dictionary<string, Type>(types.Length);

            foreach (Type type in types)
            {
                // non-public or static class
                if (!type.IsPublic || (type.IsAbstract && type.IsSealed))
                    continue;

                dict.Add(
                    type.GetUnversionedNameAssembly().Replace(
                        "NYurik.FastBinTimeseries.Legacy", "NYurik.FastBinTimeseries"), type);
            }

            return dict;
        }
    }
}