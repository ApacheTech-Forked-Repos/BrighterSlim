using System;
using System.Collections.Generic;
using ApacheTech.Common.BrighterSlim.Extensions;

namespace ApacheTech.Common.BrighterSlim
{
    public class TransformLifetimeScopeAsync : IAmATransformLifetimeAsync
    {
        private readonly IAmAMessageTransformerFactoryAsync _factory;
        private readonly IList<IAmAMessageTransformAsync> _trackedObjects = new List<IAmAMessageTransformAsync>();

        public TransformLifetimeScopeAsync(IAmAMessageTransformerFactoryAsync factory)
        {
            _factory = factory;
        }

        public void Dispose()
        {
            ReleaseTrackedObjects();
            GC.SuppressFinalize(this);
        }

        ~TransformLifetimeScopeAsync()
        {
            ReleaseTrackedObjects();
        }

        public void Add(IAmAMessageTransformAsync instance)
        {
            _trackedObjects.Add(instance);
        }

        private void ReleaseTrackedObjects()
        {
            _trackedObjects.Each((trackedItem) =>
            {
                _factory.Release(trackedItem);
            });
        }
    }
}
