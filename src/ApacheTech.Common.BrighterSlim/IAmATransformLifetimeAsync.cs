using System;

namespace ApacheTech.Common.BrighterSlim
{
    internal interface IAmATransformLifetimeAsync : IDisposable
    {
        void Add(IAmAMessageTransformAsync transform);
    }
}
