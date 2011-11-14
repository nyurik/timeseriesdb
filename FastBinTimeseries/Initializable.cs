using System;

namespace NYurik.FastBinTimeseries
{
    public abstract class Initializable
    {
        private bool _isInitialized;

        public bool IsInitialized
        {
            get { return _isInitialized; }
            // todo: make protected
            set
            {
                ThrowOnInitialized();
                _isInitialized = value;
            }
        }

        protected void ThrowOnNotInitialized()
        {
            if (!_isInitialized)
                throw new InvalidOperationException("This instance has not been initialized");
        }

        protected void ThrowOnInitialized()
        {
            if (_isInitialized)
                throw new InvalidOperationException("This instance has already been initialized");
        }
    }
}