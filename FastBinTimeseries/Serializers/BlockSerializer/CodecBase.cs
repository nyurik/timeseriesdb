namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class CodecBase
    {
        internal const int HeaderSize = 4;

        #region Debug

#if DEBUG_SERIALIZER
        private const int DebugHistLength = 10;
        private readonly Tuple<string, int, double>[] _debugDoubleHist = new Tuple<string, int, double>[DebugHistLength];
        private readonly Tuple<string, int, float>[] _debugFloatHist = new Tuple<string, int, float>[DebugHistLength];
        private readonly Tuple<string, int, long>[] _debugLongHist = new Tuple<string, int, long>[DebugHistLength];

        [UsedImplicitly]
        internal void DebugLong(long v, string name)
        {
            DebugValue(_debugLongHist, v, name);
        }

        [UsedImplicitly]
        internal void DebugFloat(float v, string name)
        {
            DebugValue(_debugFloatHist, v, name);
        }

        [UsedImplicitly]
        internal void DebugFloat(double v, string name)
        {
            DebugValue(_debugDoubleHist, v, name);
        }

        internal void DebugValue<T>(Tuple<string, int, T>[] values, T v, string name)
        {
            Array.Copy(values, 1, values, 0, values.Length - 1);
            values[values.Length - 1] = Tuple.Create(name, BufferPos, v);
        }
#endif

        #endregion
    }
}