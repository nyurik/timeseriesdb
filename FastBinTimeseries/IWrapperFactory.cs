namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// This interface is used to easily create wrapper objects without referencing the generic subtype
    /// </summary>
    public interface IWrapperFactory
    {
        TDst Create<TSrc, TDst, TSubType>(TSrc source);
    }
}