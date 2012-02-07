using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

[assembly: AssemblyTitle("FastBinTimeseries.Legacy")]
[assembly: AssemblyDescription("")]
[assembly: AssemblyProduct("FastBinTimeseries.Legacy")]
[assembly: ComVisible(false)]
[assembly: Guid("58958894-a74d-4d50-bb9e-535f5a22527c")]
[assembly: CLSCompliant(true)]

#if DEBUG

[assembly: InternalsVisibleTo("NYurik.FastBinTimeseries.Test")]

#else

[assembly: InternalsVisibleTo("NYurik.FastBinTimeseries.Test" + AssemblyVersion.Key)]

#endif