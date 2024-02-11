using System;

namespace ApacheTech.Common.BrighterSlim
{
    internal interface IAmATransformLifetime : IDisposable
    {
        void Add(IAmAMessageTransform transform);
    }
}
