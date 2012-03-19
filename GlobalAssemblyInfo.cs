using System.Reflection;
using NYurik.TimeSeriesDb.Properties;

[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("Yuri Astrakhan")]
[assembly: AssemblyCopyright("Copyright © Yuri Astrakhan 2009-2012")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]
[assembly: AssemblyVersion(AssemblyVersion.Ver)]
[assembly: AssemblyFileVersion(AssemblyVersion.Ver)]

#if SIGN

#pragma warning disable 1699
[assembly: AssemblyKeyFile(@"..\..\..\key.snk")]
#pragma warning restore 1699

#endif
