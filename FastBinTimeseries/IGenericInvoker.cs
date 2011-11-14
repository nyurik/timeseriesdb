namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// The implementor of this interface can execute <see cref="IGenericCallable{TDst,TArg}.Run{T}"/> method.
    /// </summary>
    public interface IGenericInvoker
    {
        /// <summary>
        /// Calls a factory method without explicitly specifying the type of the sub-item.
        /// </summary>
        TDst RunGenericMethod<TDst, TArg>(IGenericCallable<TDst, TArg> callable, TArg arg);
    }

    /// <summary>
    /// This interface is used to run a generic method without referencing the generic subtype
    /// </summary>
    public interface IGenericCallable<out TDst, in TArg>
    {
        TDst Run<T>(IGenericInvoker source, TArg arg);
    }
}