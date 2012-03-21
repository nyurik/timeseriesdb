using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using NYurik.TimeSeriesDb.Properties;

[assembly: AssemblyTitle("TimeSeriesDb")]
[assembly:
    AssemblyDescription(
        "TimeSeriesDb is a flat-file binary data storage library optimized for storing ordered timeseries in a compressed (deltas) and non-compressed forms"
        )]
[assembly: AssemblyProduct("TimeSeriesDb")]
[assembly: ComVisible(false)]
[assembly: Guid("58958894-a74d-4d50-bb9e-535f5a22527c")]
[assembly: CLSCompliant(true)]

#if SIGN

[assembly: InternalsVisibleTo("NYurik.TimeSeriesDb.Test" + AssemblyVersion.Key)]
[assembly: InternalsVisibleTo("NYurik.TimeSeriesDb.Legacy" + AssemblyVersion.Key)]

#else

[assembly: InternalsVisibleTo("NYurik.TimeSeriesDb.Test")]
[assembly: InternalsVisibleTo("NYurik.TimeSeriesDb.Legacy")]

#endif

namespace NYurik.TimeSeriesDb.Properties
{
    internal static class AssemblyVersion
    {
        public const string Ver = "1.127"; // The second number should match the SVN revision

#if SIGN

        public const string Key =
            ", PublicKey=" +
            "00240000048000009400000006020000002400005253413100040000010001002db22c4c39a1fd" +
            "d5d05873819687e4f3054dbd28456561de625979187fbff7da4d4d069d3d5a33fed33616d14809" +
            "458fe87cb6bbb84177835cfeeb9240002788855f5b22cc8841f53a5b2b91ae0e463aa9955bbf00" +
            "693cf161ff5d43173e4899bac361fd95f63ed3b98096a28298d12374c79147fceade4565e85b89" +
            "44bffbc2";

#endif
    }
}