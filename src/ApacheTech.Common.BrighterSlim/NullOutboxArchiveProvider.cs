using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ApacheTech.Common.BrighterSlim
{
    /// <summary>
    /// Use this archiver will result in messages just being deleted from the outbox and not stored
    /// </summary>
    public class NullOutboxArchiveProvider : IAmAnArchiveProvider
    {
        public NullOutboxArchiveProvider()
        {
        }

        public void ArchiveMessage(Message message)
        {
        }

        public Task ArchiveMessageAsync(Message message, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task<Guid[]> ArchiveMessagesAsync(Message[] messages, CancellationToken cancellationToken)
        {
            return Task.FromResult(messages.Select(m => m.Id).ToArray());
        }
    }
}
