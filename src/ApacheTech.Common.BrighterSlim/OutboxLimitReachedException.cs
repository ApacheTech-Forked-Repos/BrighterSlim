using System;

namespace ApacheTech.Common.BrighterSlim
{
    public class OutboxLimitReachedException : Exception
    {
        public OutboxLimitReachedException() { }

        public OutboxLimitReachedException(string message) : base(message) { }

        public OutboxLimitReachedException(string message, Exception innerException) : base(message, innerException) { }
    }
}
