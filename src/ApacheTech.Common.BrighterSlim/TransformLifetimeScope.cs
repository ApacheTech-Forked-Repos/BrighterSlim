using System;
using System.Collections.Generic;
using ApacheTech.Common.BrighterSlim.Extensions;

namespace ApacheTech.Common.BrighterSlim
{
    public class TransformLifetimeScope : IAmATransformLifetime
    {
        private readonly IAmAMessageTransformerFactory _factory;
        private readonly IList<IAmAMessageTransform> _trackedObjects = new List<IAmAMessageTransform>();

        public TransformLifetimeScope(IAmAMessageTransformerFactory factory)
        {
            _factory = factory;
        }

        public void Dispose()
        {
            ReleaseTrackedObjects();
            GC.SuppressFinalize(this);
        }

        ~TransformLifetimeScope()
        {
            ReleaseTrackedObjects();
        }

        public void Add(IAmAMessageTransform instance)
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
