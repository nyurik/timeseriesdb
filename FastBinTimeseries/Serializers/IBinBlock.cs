namespace NYurik.FastBinTimeseries.Serializers
{
    public interface IBinBlock<THeader, TItem>
    {
        THeader Header { get; set; }
        TItem[] Items { get; set; }
    }
}