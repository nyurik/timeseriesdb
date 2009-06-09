using System;
using System.Reflection;

namespace NYurik.FastBinTimeseries.CommonCode
{
    public static class MiscUtilities
    {
        /// <summary>
        /// Get a single attribute (or null) of a given type attached to a value.
        /// The value might be a <see cref="Type"/> object or Property/Method/... info acquired through reflection.
        /// An exception is thrown if more than one attribute of a given type was found.
        /// </summary>
        /// <typeparam name="TAttr">Type of the attribute to get</typeparam>
        /// <param name="customAttrProvider">Enum value</param>
        /// <returns>An attribute object or null if not found</returns>
        public static TAttr ExtractSingleAttribute<TAttr>(this ICustomAttributeProvider customAttrProvider)
            where TAttr : Attribute
        {
            object[] attributes = customAttrProvider.GetCustomAttributes(typeof (TAttr), true);
            if (attributes.Length > 0)
            {
                if (attributes.Length > 1)
                    throw new ArgumentException(
                        String.Format("Found {0} (>1) attributes {1} detected for {2}", attributes.Length,
                                      typeof (TAttr).Name, customAttrProvider));
                return (TAttr) attributes[0];
            }
            return null;
        }
    }
}