using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using NYurik.FastBinTimeseries.Properties;

[assembly: AssemblyTitle("FastBinTimeseries.Legacy")]
[assembly: AssemblyDescription("")]
[assembly: AssemblyProduct("FastBinTimeseries.Legacy")]
[assembly: ComVisible(false)]
[assembly: Guid("58958894-a74d-4d50-bb9e-535f5a22527c")]
[assembly: CLSCompliant(true)]

#if SIGN

[assembly: InternalsVisibleTo("NYurik.FastBinTimeseries.Test" + AssemblyVersion.Key)]

#else

[assembly: InternalsVisibleTo("NYurik.FastBinTimeseries.Test")]

#endif