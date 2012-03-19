using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using NYurik.TimeSeriesDb.Properties;

[assembly: AssemblyTitle("TimeSeriesDb.Legacy")]
[assembly: AssemblyDescription("")]
[assembly: AssemblyProduct("TimeSeriesDb.Legacy")]
[assembly: ComVisible(false)]
[assembly: Guid("58958894-a74d-4d50-bb9e-535f5a22527c")]
[assembly: CLSCompliant(true)]

#if SIGN

[assembly: InternalsVisibleTo("NYurik.TimeSeriesDb.Test" + AssemblyVersion.Key)]

#else

[assembly: InternalsVisibleTo("NYurik.TimeSeriesDb.Test")]

#endif