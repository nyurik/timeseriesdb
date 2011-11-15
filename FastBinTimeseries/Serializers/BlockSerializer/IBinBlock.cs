namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public interface IBinBlock<THeader, TItem>
    {
        THeader Header { get; set; }
        TItem[] Items { get; set; }
    }
}