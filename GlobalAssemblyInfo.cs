using System.Reflection;

[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("")]
[assembly: AssemblyCopyright("Copyright © Yuri Astrakhan 2009-2012")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]
[assembly: AssemblyVersion(AssemblyVersion.Ver)]
[assembly: AssemblyFileVersion(AssemblyVersion.Ver)]

#if !DEBUG

#pragma warning disable 1699
[assembly: AssemblyKeyFile(@"..\..\..\key.snk")]
#pragma warning restore 1699

#endif
