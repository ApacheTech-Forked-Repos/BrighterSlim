using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ApacheTech.Common.BrighterSlim
{
    /// <summary>
    /// Provide services to CommandProcessor that persist across the lifetime of the application. Allows separation from elements that have a lifetime linked
    /// to the scope of a request, or are transient for DI purposes
    /// </summary>
    public class ExternalBusServices<TMessage, TTransaction> : IAmAnExternalBusService
        where TMessage : Message
    {
        private readonly IAmAnOutboxSync<TMessage, TTransaction> _outBox;
        private readonly IAmAnOutboxAsync<TMessage, TTransaction> _asyncOutbox;
        private readonly int _outboxTimeout;
        private readonly int _outboxBulkChunkSize;
        private readonly IAmAProducerRegistry _producerRegistry;

        private static readonly SemaphoreSlim s_clearSemaphoreToken = new SemaphoreSlim(1, 1);
        private static readonly SemaphoreSlim s_backgroundClearSemaphoreToken = new SemaphoreSlim(1, 1);
        //Used to checking the limit on outstanding messages for an Outbox. We throw at that point. Writes to the static bool should be made thread-safe by locking the object
        private static readonly SemaphoreSlim s_checkOutstandingSemaphoreToken = new SemaphoreSlim(1, 1);

        private const string ADDMESSAGETOOUTBOX = "Add message to outbox";
        private const string GETMESSAGESFROMOUTBOX = "Get outstanding messages from the outbox";
        private const string DISPATCHMESSAGE = "Dispatching message";
        private const string BULKDISPATCHMESSAGE = "Bulk dispatching messages";

        private DateTime _lastOutStandingMessageCheckAt = DateTime.UtcNow;

        //Uses -1 to indicate no outbox and will thus force a throw on a failed publish
        private int _outStandingCount;
        private bool _disposed;

        /// <summary>
        /// Creates an instance of External Bus Services
        /// </summary>
        /// <param name="producerRegistry">A registry of producers</param>
        /// <param name="policyRegistry">A registry for reliability policies</param>
        /// <param name="outbox">An outbox for transactional messaging, if none is provided, use an InMemoryOutbox</param>
        /// <param name="outboxBulkChunkSize">The size of a chunk for bulk work</param>
        /// <param name="outboxTimeout">How long to timeout for with an outbox</param>
        public ExternalBusServices(
            IAmAProducerRegistry producerRegistry,
            IAmAnOutbox outbox = null,
            int outboxBulkChunkSize = 100,
            int outboxTimeout = 300
            )
        {
            _producerRegistry = producerRegistry ?? throw new ConfigurationException("Missing Producer Registry for External Bus Services");

            //default to in-memory; expectation for a in memory box is Message and CommittableTransaction
            if (outbox == null)
                outbox = new InMemoryOutbox() as IAmAnOutbox;
            if (outbox is IAmAnOutboxSync<TMessage, TTransaction> syncOutbox)
                _outBox = syncOutbox;
            if (outbox is IAmAnOutboxAsync<TMessage, TTransaction> asyncOutbox)
                _asyncOutbox = asyncOutbox;
            _outboxBulkChunkSize = outboxBulkChunkSize;
            _outboxTimeout = outboxTimeout;

            ConfigureCallbacks();
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing && _producerRegistry != null)
                _producerRegistry.CloseAll();
            _disposed = true;
        }

        /// <summary>
        /// Adds a message to the outbox
        /// </summary>
        /// <param name="request">The request that we are storing (used for id)</param>
        /// <param name="message">The message to store in the outbox</param>
        /// <param name="overridingTransactionProvider">The provider of the transaction for the outbox</param>
        /// <param name="continueOnCapturedContext">Use the same thread for a callback</param>
        /// <param name="cancellationToken">Allow cancellation of the message</param>
        /// <typeparam name="TRequest">The type of request we are saving</typeparam>
        /// <exception cref="ChannelFailureException">Thrown if we cannot write to the outbox</exception>
        public async Task AddToOutboxAsync<TRequest>(
            TRequest request,
            TMessage message,
            IAmABoxTransactionProvider<TTransaction> overridingTransactionProvider = null,
            bool continueOnCapturedContext = false,
            CancellationToken cancellationToken = default) where TRequest : IRequest
        {
            CheckOutboxOutstandingLimit();

            try
            {

                await _asyncOutbox.AddAsync(message, _outboxTimeout, cancellationToken, overridingTransactionProvider)
                    .ConfigureAwait(continueOnCapturedContext);
            }
            catch (Exception ex)
            {
                throw new ChannelFailureException($"Could not write request {request.Id} to the outbox", ex);
            }
            Activity.Current?.AddEvent(new ActivityEvent(ADDMESSAGETOOUTBOX,
                tags: new ActivityTagsCollection { { "MessageId", message.Id } }));
        }

        /// <summary>
        /// Adds a message to the outbox
        /// </summary>
        /// <param name="messages">The messages to store in the outbox</param>
        /// <param name="overridingTransactionProvider"></param>
        /// <param name="continueOnCapturedContext">Use the same thread for a callback</param>
        /// <param name="cancellationToken">Allow cancellation of the message</param>
        /// <param name="overridingTransactionProvider ">The provider of the transaction for the outbox</param>
        /// <exception cref="ChannelFailureException">Thrown if we cannot write to the outbox</exception>
        public async Task AddToOutboxAsync<TTransaction>(
            IEnumerable<TMessage> messages,
            IAmABoxTransactionProvider<TTransaction> overridingTransactionProvider = null,
            bool continueOnCapturedContext = false,
            CancellationToken cancellationToken = default)
        {
            CheckOutboxOutstandingLimit();

#pragma warning disable CS0618
            if (_asyncOutbox is IAmABulkOutboxAsync<TMessage, TTransaction> box)
#pragma warning restore CS0618
            {
                foreach (var chunk in ChunkMessages(messages))
                {
                    try
                    {
                        await box.AddAsync(chunk, _outboxTimeout, cancellationToken, overridingTransactionProvider)
                                .ConfigureAwait(continueOnCapturedContext);
                    }
                    catch (Exception ex)
                    {
                        throw new ChannelFailureException($"Could not write {chunk.Count()} messages to the outbox", ex);
                    }
                }
            }
            else
            {
                throw new InvalidOperationException($"{_asyncOutbox.GetType()} does not implement IAmABulkOutboxAsync");
            }
        }

        /// <summary>
        /// Adds a message to the outbox
        /// </summary>
        /// <param name="request">The request the message is composed from (used for diagnostics)</param>
        /// <param name="message">The message we intend to send</param>
        /// <param name="overridingTransactionProvider">A transaction provider that gives us the transaction to use with the Outbox</param>
        /// <typeparam name="TTransaction">The transaction type for the Outbox</typeparam>
        /// <typeparam name="TRequest">The type of the request we have converted into a message</typeparam>
        /// <exception cref="ChannelFailureException"></exception>
        public void AddToOutbox<TRequest>(
            TRequest request,
            TMessage message,
            IAmABoxTransactionProvider<TTransaction> overridingTransactionProvider = null)
            where TRequest : class, IRequest
        {
            CheckOutboxOutstandingLimit();

            try
            {
                _outBox.Add(message, _outboxTimeout, overridingTransactionProvider);
            }
            catch (Exception ex)
            {
                throw new ChannelFailureException($"Could not write request {request.Id} to the outbox", ex);
            }
            Activity.Current?.AddEvent(new ActivityEvent(ADDMESSAGETOOUTBOX,
                tags: new ActivityTagsCollection { { "MessageId", message.Id } }));
        }

        public void AddToOutbox<TTransaction>(
            IEnumerable<TMessage> messages,
            IAmABoxTransactionProvider<TTransaction> overridingTransactionProvider = null
            )
        {
            CheckOutboxOutstandingLimit();

#pragma warning disable CS0618
            if (_outBox is IAmABulkOutboxSync<TMessage, TTransaction> box)
#pragma warning restore CS0618
            {
                foreach (var chunk in ChunkMessages(messages))
                {
                    try
                    {
                        box.Add(chunk, _outboxTimeout, overridingTransactionProvider);
                    }
                    catch (Exception ex)
                    {
                        throw new ChannelFailureException($"Could not write {chunk.Count()} messages to the outbox", ex);
                    }
                }
            }
            else
            {
                throw new InvalidOperationException($"{_outBox.GetType()} does not implement IAmABulkOutboxSync");
            }
        }

        /// <summary>
        /// Used with RPC to call a remote service via the external bus
        /// </summary>
        /// <param name="outMessage">The message to send</param>
        /// <typeparam name="T">The type of the call</typeparam>
        /// <typeparam name="TResponse">The type of the response</typeparam>
        public void CallViaExternalBus<T, TResponse>(Message outMessage)
            where T : class, ICall where TResponse : class, IResponse
        {
            //We assume that this only occurs over a blocking producer
            var producer = _producerRegistry.LookupByOrDefault(outMessage.Header.Topic);
            if (producer is IAmAMessageProducerSync producerSync)
                producerSync.Send(outMessage);
        }

        /// <summary>
        /// This is the clear outbox for explicit clearing of messages.
        /// </summary>
        /// <param name="posts">The ids of the posts that you would like to clear</param>
        /// <exception cref="InvalidOperationException">Thrown if there is no async outbox defined</exception>
        /// <exception cref="NullReferenceException">Thrown if a message cannot be found</exception>
        public void ClearOutbox(params Guid[] posts)
        {
            if (!HasOutbox())
                throw new InvalidOperationException("No outbox defined.");

            // Only allow a single Clear to happen at a time
            s_clearSemaphoreToken.Wait();
            try
            {
                foreach (var messageId in posts)
                {
                    var message = _outBox.Get(messageId);
                    if (message == null || message.Header.MessageType == MessageType.MT_NONE)
                        throw new NullReferenceException($"Message with Id {messageId} not found in the Outbox");

                    Dispatch(new[] { message });
                }
            }
            finally
            {
                s_clearSemaphoreToken.Release();
            }

            CheckOutstandingMessages();
        }

        /// <summary>
        /// This is the clear outbox for explicit clearing of messages.
        /// </summary>
        /// <param name="posts">The ids of the posts that you would like to clear</param>
        /// <param name="continueOnCapturedContext">Should we use the same thread in the callback</param>
        /// <param name="cancellationToken">Allow cancellation of the operation</param>
        /// <exception cref="InvalidOperationException">Thrown if there is no async outbox defined</exception>
        /// <exception cref="NullReferenceException">Thrown if a message cannot be found</exception>
        public async Task ClearOutboxAsync(
            IEnumerable<Guid> posts,
            bool continueOnCapturedContext = false,
            CancellationToken cancellationToken = default)
        {
            if (!HasAsyncOutbox())
                throw new InvalidOperationException("No async outbox defined.");

            await s_clearSemaphoreToken.WaitAsync(cancellationToken);
            try
            {
                foreach (var messageId in posts)
                {
                    var message = await _asyncOutbox.GetAsync(messageId, _outboxTimeout, cancellationToken);
                    if (message == null || message.Header.MessageType == MessageType.MT_NONE)
                        throw new NullReferenceException($"Message with Id {messageId} not found in the Outbox");

                    await DispatchAsync(new[] { message }, continueOnCapturedContext, cancellationToken);
                }
            }
            finally
            {
                s_clearSemaphoreToken.Release();
            }

            CheckOutstandingMessages();
        }

        /// <summary>
        /// This is the clear outbox for explicit clearing of messages.
        /// </summary>
        /// <param name="amountToClear">Maximum number to clear.</param>
        /// <param name="minimumAge">The minimum age of messages to be cleared in milliseconds.</param>
        /// <param name="useAsync">Use the Async outbox and Producer</param>
        /// <param name="useBulk">Use bulk sending capability of the message producer, this must be paired with useAsync.</param>
        /// <param name="args">Optional bag of arguments required by an outbox implementation to sweep</param>
        public void ClearOutbox(
            int amountToClear,
            int minimumAge,
            bool useAsync,
            bool useBulk,
            Dictionary<string, object> args = null)
        {
            var span = Activity.Current;
            span?.AddTag("amountToClear", amountToClear);
            span?.AddTag("minimumAge", minimumAge);
            span?.AddTag("async", useAsync);
            span?.AddTag("bulk", useBulk);

            if (useAsync)
            {
                if (!HasAsyncOutbox())
                    throw new InvalidOperationException("No async outbox defined.");

                Task.Run(() => BackgroundDispatchUsingAsync(amountToClear, minimumAge, useBulk, args),
                    CancellationToken.None);
            }

            else
            {
                if (!HasOutbox())
                    throw new InvalidOperationException("No outbox defined.");

                Task.Run(() => BackgroundDispatchUsingSync(amountToClear, minimumAge, args));
            }
        }

        /// <summary>
        /// Configure the callbacks for the producers 
        /// </summary>
        private void ConfigureCallbacks()
        {
            //Only register one, to avoid two callbacks where we support both interfaces on a producer
            foreach (var producer in _producerRegistry.Producers)
            {
                if (!ConfigurePublisherCallbackMaybe(producer))
                    ConfigureAsyncPublisherCallbackMaybe(producer);
            }
        }

        /// <summary>
        /// If a producer supports a callback then we can use this to mark a message as dispatched in an asynchronous
        /// Outbox
        /// </summary>
        /// <param name="producer">The producer to add a callback for</param>
        /// <returns></returns>
        private bool ConfigureAsyncPublisherCallbackMaybe(IAmAMessageProducer producer)
        {
            if (producer is ISupportPublishConfirmation producerSync)
            {
                producerSync.OnMessagePublished += async delegate (bool success, Guid id)
                {
                    if (success)
                    {
                        if (_asyncOutbox != null)
                            await _asyncOutbox.MarkDispatchedAsync(id, DateTime.UtcNow);
                    }
                };
                return true;
            }

            return false;
        }

        /// <summary>
        /// If a producer supports a callback then we can use this to mark a message as dispatched in a synchronous
        /// Outbox
        /// </summary>
        /// <param name="producer">The producer to add a callback for</param>
        private bool ConfigurePublisherCallbackMaybe(IAmAMessageProducer producer)
        {
            if (producer is ISupportPublishConfirmation producerSync)
            {
                producerSync.OnMessagePublished += delegate (bool success, Guid id)
                {
                    if (success)
                    {
                        if (_outBox != null)
                            _outBox.MarkDispatched(id, DateTime.UtcNow);
                    }
                };
                return true;
            }

            return false;
        }

        /// <summary>
        /// Do we have an async outbox defined?
        /// </summary>
        /// <returns>true if defined</returns>
        public bool HasAsyncOutbox()
        {
            return _asyncOutbox != null;
        }

        /// <summary>
        /// Do we have an async bulk outbox defined?
        /// </summary>
        /// <returns>true if defined</returns>
        public bool HasAsyncBulkOutbox()
        {
#pragma warning disable CS0618
            return _asyncOutbox is IAmABulkOutboxAsync<TMessage, TTransaction>;
#pragma warning restore CS0618
        }

        /// <summary>
        /// Do we have a synchronous outbox defined?
        /// </summary>
        /// <returns>true if defined</returns>
        public bool HasOutbox()
        {
            return _outBox != null;
        }

        /// <summary>
        /// Do we have a synchronous bulk outbox defined?
        /// </summary>
        /// <returns>true if defined</returns>
        public bool HasBulkOutbox()
        {
#pragma warning disable CS0618
            return _outBox is IAmABulkOutboxSync<TMessage, TTransaction>;
#pragma warning restore CS0618
        }

        private IEnumerable<List<TMessage>> ChunkMessages(IEnumerable<TMessage> messages)
        {
            return Enumerable.Range(0, (int)Math.Ceiling(messages.Count() / (decimal)_outboxBulkChunkSize))
                .Select(i => new List<TMessage>(messages
                    .Skip(i * _outboxBulkChunkSize)
                    .Take(_outboxBulkChunkSize)
                    .ToArray()));
        }

        private void CheckOutboxOutstandingLimit()
        {
            bool hasOutBox = _outBox != null || _asyncOutbox != null;
            if (!hasOutBox)
                return;

            int maxOutStandingMessages = _producerRegistry.GetDefaultProducer().MaxOutStandingMessages;

            // Because a thread recalculates this, we may always be in a delay, so we check on entry for the next outstanding item
            bool exceedsOutstandingMessageLimit =
                maxOutStandingMessages != -1 && _outStandingCount > maxOutStandingMessages;

            if (exceedsOutstandingMessageLimit)
                throw new OutboxLimitReachedException(
                    $"The outbox limit of {maxOutStandingMessages} has been exceeded");
        }

        private void CheckOutstandingMessages()
        {
            var now = DateTime.UtcNow;
            var checkInterval =
                TimeSpan.FromMilliseconds(_producerRegistry.GetDefaultProducer()
                    .MaxOutStandingCheckIntervalMilliSeconds);


            var timeSinceLastCheck = now - _lastOutStandingMessageCheckAt;

            if (timeSinceLastCheck < checkInterval)
            {
                return;
            }

            //This is expensive, so use a background thread
            Task.Run(() => OutstandingMessagesCheck());
        }


        private async Task BackgroundDispatchUsingSync(int amountToClear, int minimumAge,
            Dictionary<string, object> args)
        {
            var span = Activity.Current;
            if (await s_backgroundClearSemaphoreToken.WaitAsync(TimeSpan.Zero))
            {
                await s_clearSemaphoreToken.WaitAsync(CancellationToken.None);
                try
                {
                    var messages = _outBox.OutstandingMessages(minimumAge, amountToClear, args: args);
                    span?.AddEvent(new ActivityEvent(GETMESSAGESFROMOUTBOX,
                        tags: new ActivityTagsCollection { { "Outstanding Messages", messages.Count() } }));
                    Dispatch(messages);
                    span?.SetStatus(ActivityStatusCode.Ok);
                }
                catch (Exception e)
                {
                    span?.SetStatus(ActivityStatusCode.Error, "Error while dispatching from outbox");
                }
                finally
                {
                    span?.Dispose();
                    s_clearSemaphoreToken.Release();
                    s_backgroundClearSemaphoreToken.Release();
                }

                CheckOutstandingMessages();
            }
            else
            {
                span?.SetStatus(ActivityStatusCode.Error)
                    .Dispose();
            }
        }

        private async Task BackgroundDispatchUsingAsync(int amountToClear, int minimumAge, bool useBulk,
            Dictionary<string, object> args)
        {
            var span = Activity.Current;
            if (await s_backgroundClearSemaphoreToken.WaitAsync(TimeSpan.Zero))
            {
                await s_clearSemaphoreToken.WaitAsync(CancellationToken.None);
                try
                {
                    var messages =
                        await _asyncOutbox.OutstandingMessagesAsync(minimumAge, amountToClear, args: args);
                    span?.AddEvent(new ActivityEvent(GETMESSAGESFROMOUTBOX));


                    if (useBulk)
                    {
                        await BulkDispatchAsync(messages, CancellationToken.None);
                    }
                    else
                    {
                        await DispatchAsync(messages, false, CancellationToken.None);
                    }

                    span?.SetStatus(ActivityStatusCode.Ok);
                }
                catch (Exception e)
                {
                    span?.SetStatus(ActivityStatusCode.Error, "Error while dispatching from outbox");
                }
                finally
                {
                    span?.Dispose();
                    s_clearSemaphoreToken.Release();
                    s_backgroundClearSemaphoreToken.Release();
                }

                CheckOutstandingMessages();
            }
            else
            {
                span?.SetStatus(ActivityStatusCode.Error)
                    .Dispose();
            }
        }

        private void Dispatch(IEnumerable<Message> posts)
        {
            foreach (var message in posts)
            {
                Activity.Current?.AddEvent(new ActivityEvent(DISPATCHMESSAGE,
                    tags: new ActivityTagsCollection
                    {
                        { "Topic", message.Header.Topic }, { "MessageId", message.Id }
                    }));

                var producer = _producerRegistry.LookupByOrDefault(message.Header.Topic);

                if (producer is IAmAMessageProducerSync producerSync)
                {
                    //mark dispatch handled by a callback - set in constructor
                    producerSync.Send(message);
                    if (producer is not ISupportPublishConfirmation)
                    {
                        _outBox.MarkDispatched(message.Id, DateTime.UtcNow);
                    }
                }
                else
                    throw new InvalidOperationException("No sync message producer defined.");
            }
        }

        private async Task DispatchAsync(IEnumerable<Message> posts, bool continueOnCapturedContext,
            CancellationToken cancellationToken)
        {
            foreach (var message in posts)
            {
                Activity.Current?.AddEvent(new ActivityEvent(DISPATCHMESSAGE,
                    tags: new ActivityTagsCollection
                    {
                        { "Topic", message.Header.Topic }, { "MessageId", message.Id }
                    }));

                var producer = _producerRegistry.LookupByOrDefault(message.Header.Topic);

                if (producer is IAmAMessageProducerAsync producerAsync)
                {
                    if (producer is ISupportPublishConfirmation)
                    {
                        //mark dispatch handled by a callback - set in constructor
                        await producerAsync.SendAsync(message).ConfigureAwait(continueOnCapturedContext);
                    }
                    else
                    {
                        await producerAsync.SendAsync(message).ConfigureAwait(continueOnCapturedContext);
                        await _asyncOutbox.MarkDispatchedAsync(message.Id, DateTime.UtcNow,
                                cancellationToken: cancellationToken);
                    }
                }
                else
                    throw new InvalidOperationException("No async message producer defined.");
            }
        }

        private async Task BulkDispatchAsync(IEnumerable<Message> posts, CancellationToken cancellationToken)
        {
            var span = Activity.Current;
            //Chunk into Topics
            var messagesByTopic = posts.GroupBy(m => m.Header.Topic);

            foreach (var topicBatch in messagesByTopic)
            {
                var producer = _producerRegistry.LookupByOrDefault(topicBatch.Key);

                if (producer is IAmABulkMessageProducerAsync bulkMessageProducer)
                {
                    var messages = topicBatch.ToArray();
                    span?.AddEvent(new ActivityEvent(BULKDISPATCHMESSAGE,
                        tags: new ActivityTagsCollection
                        {
                            { "Topic", topicBatch.Key }, { "Number Of Messages", messages.Length }
                        }));
                    var dispatchesMessages = bulkMessageProducer.SendAsync(messages, cancellationToken);

                    await foreach (var successfulMessage in dispatchesMessages.WithCancellation(cancellationToken))
                    {
                        if (!(producer is ISupportPublishConfirmation))
                        {
                            await _asyncOutbox.MarkDispatchedAsync(successfulMessage,
                                    DateTime.UtcNow, cancellationToken: cancellationToken);
                        }
                    }
                }
                else
                {
                    throw new InvalidOperationException("No async bulk message producer defined.");
                }
            }
        }

        private void OutstandingMessagesCheck()
        {
            s_checkOutstandingSemaphoreToken.Wait();

            _lastOutStandingMessageCheckAt = DateTime.UtcNow;
            try
            {
                var producer = _producerRegistry.GetDefaultProducer();
                if (_outBox != null)
                {
                    _outStandingCount = _outBox
                        .OutstandingMessages(
                            producer.MaxOutStandingCheckIntervalMilliSeconds,
                            args: producer.OutBoxBag
                        )
                        .Count();
                    return;
                }

                _outStandingCount = 0;
            }
            catch (Exception ex)
            {
                //if we can't talk to the outbox, we would swallow the exception on this thread
                //by setting the _outstandingCount to -1, we force an exception
                _outStandingCount = 0;
            }
            finally
            {
                s_checkOutstandingSemaphoreToken.Release();
            }
        }
    }
}
