﻿#region Licence

/* The MIT License (MIT)
Copyright © 2014 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */

#endregion

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using ApacheTech.Common.BrighterSlim.FeatureSwitch;
using Exception = System.Exception;

namespace ApacheTech.Common.BrighterSlim
{
    /// <summary>
    /// Class CommandProcessor.
    /// Implements both the <a href="http://www.hillside.net/plop/plop2001/accepted_submissions/PLoP2001/bdupireandebfernandez0/PLoP2001_bdupireandebfernandez0_1.pdf">Command Dispatcher</a> 
    /// and <a href="http://wiki.hsr.ch/APF/files/CommandProcessor.pdf">Command Processor</a> Design Patterns 
    /// </summary>
    public class CommandProcessor : IAmACommandProcessor
    {
        private readonly IAmASubscriberRegistry _subscriberRegistry;
        private readonly IAmAHandlerFactorySync _handlerFactorySync;
        private readonly IAmAHandlerFactoryAsync _handlerFactoryAsync;
        private readonly IAmARequestContextFactory _requestContextFactory;
        private readonly InboxConfiguration _inboxConfiguration;
        private readonly IAmAFeatureSwitchRegistry _featureSwitchRegistry;
        private readonly IEnumerable<Subscription> _replySubscriptions;
        private readonly TransformPipelineBuilder _transformPipelineBuilder;
        private readonly TransformPipelineBuilderAsync _transformPipelineBuilderAsync;

        //Uses -1 to indicate no outbox and will thus force a throw on a failed publish

        // the following are not readonly to allow setting them to null on dispose
        private readonly IAmAChannelFactory _responseChannelFactory;

        private const string PROCESSCOMMAND = "Process Command";
        private const string PROCESSEVENT = "Process Event";
        private const string DEPOSITPOST = "Deposit Post";

        /// <summary>
        /// Use this as an identifier for your <see cref="Policy"/> that determines for how long to break the circuit when communication with the Work Queue fails.
        /// Register that policy with your <see cref="IPolicyRegistry{TKey}"/> such as <see cref="PolicyRegistry"/>
        /// You can use this an identifier for you own policies, if your generic policy is the same as your Work Queue policy.
        /// </summary>
        public const string CIRCUITBREAKER = "Paramore.Brighter.CommandProcessor.CircuitBreaker";

        /// <summary>
        /// Use this as an identifier for your <see cref="Policy"/> that determines the retry strategy when communication with the Work Queue fails.
        /// Register that policy with your <see cref="IAmAPolicyRegistry"/> such as <see cref="PolicyRegistry"/>
        /// You can use this an identifier for you own policies, if your generic policy is the same as your Work Queue policy.
        /// </summary>
        public const string RETRYPOLICY = "Paramore.Brighter.CommandProcessor.RetryPolicy";

        /// <summary>
        /// We want to use double lock to let us pass parameters to the constructor from the first instance
        /// </summary>
        private static IAmAnExternalBusService _bus = null;
        private static readonly object padlock = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="CommandProcessor"/> class
        /// NO EXTERNAL BUS: Use this constructor when no external bus is required
        /// </summary>
        /// <param name="subscriberRegistry">The subscriber registry.</param>
        /// <param name="handlerFactory">The handler factory.</param>
        /// <param name="requestContextFactory">The request context factory.</param>
        /// <param name="featureSwitchRegistry">The feature switch config provider.</param>
        /// <param name="inboxConfiguration">Do we want to insert an inbox handler into pipelines without the attribute. Null (default = no), yes = how to configure</param>
        public CommandProcessor(
            IAmASubscriberRegistry subscriberRegistry,
            IAmAHandlerFactory handlerFactory,
            IAmARequestContextFactory requestContextFactory,
            IAmAFeatureSwitchRegistry featureSwitchRegistry = null,
            InboxConfiguration inboxConfiguration = null)
        {
            _subscriberRegistry = subscriberRegistry;

            if (HandlerFactoryIsNotEitherIAmAHandlerFactorySyncOrAsync(handlerFactory))
                throw new ArgumentException(
                    "No HandlerFactory has been set - either an instance of IAmAHandlerFactorySync or IAmAHandlerFactoryAsync needs to be set");

            if (handlerFactory is IAmAHandlerFactorySync handlerFactorySync)
                _handlerFactorySync = handlerFactorySync;
            if (handlerFactory is IAmAHandlerFactoryAsync handlerFactoryAsync)
                _handlerFactoryAsync = handlerFactoryAsync;

            _requestContextFactory = requestContextFactory;
            _featureSwitchRegistry = featureSwitchRegistry;
            _inboxConfiguration = inboxConfiguration;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CommandProcessor"/> class.
        /// EXTERNAL BUS AND INTERNAL BUS: Use this constructor when both external bus and command processor support is required
        /// OPTIONAL RPC: You can use this if you want to use the command processor as a client to an external bus, but also want to support RPC
        /// </summary>
        /// <param name="subscriberRegistry">The subscriber registry.</param>
        /// <param name="handlerFactory">The handler factory.</param>
        /// <param name="requestContextFactory">The request context factory.</param>
        /// <param name="policyRegistry">The policy registry.</param>
        /// <param name="bus">The external service bus that we want to send messages over</param>
        /// <param name="mapperRegistry">The mapper registry; it should also implement IAmAMessageMapperRegistryAsync</param>
        /// <param name="mapperRegistryAsync">The async mapper registry</param>
        /// <param name="featureSwitchRegistry">The feature switch config provider.</param>
        /// <param name="inboxConfiguration">Do we want to insert an inbox handler into pipelines without the attribute. Null (default = no), yes = how to configure</param>
        /// <param name="messageTransformerFactory">The factory used to create a transformer pipeline for a message mapper</param>
        /// <param name="messageTransformerFactoryAsync">The factory used to create a transformer pipeline for an async message mapper</param>
        /// <param name="replySubscriptions">The Subscriptions for creating the reply queues</param>
        /// <param name="responseChannelFactory">If we are expecting a response, then we need a channel to listen on</param>
        public CommandProcessor(
            IAmASubscriberRegistry subscriberRegistry,
            IAmAHandlerFactory handlerFactory,
            IAmARequestContextFactory requestContextFactory,
            IAmAnExternalBusService bus,
            IAmAMessageMapperRegistry mapperRegistry = null,
            IAmAFeatureSwitchRegistry featureSwitchRegistry = null,
            InboxConfiguration inboxConfiguration = null,
            IAmAMessageTransformerFactory messageTransformerFactory = null,
            IAmAMessageTransformerFactoryAsync messageTransformerFactoryAsync = null,
            IEnumerable<Subscription> replySubscriptions = null,
            IAmAChannelFactory responseChannelFactory = null
            )
            : this(subscriberRegistry, handlerFactory, requestContextFactory, featureSwitchRegistry, inboxConfiguration)
        {
            _responseChannelFactory = responseChannelFactory;
            _replySubscriptions = replySubscriptions;

            if (mapperRegistry == null)
                throw new ConfigurationException("A Command Processor with an external bus must have a message mapper registry that implements IAmAMessageMapperRegistry");
            if (!(mapperRegistry is IAmAMessageMapperRegistryAsync mapperRegistryAsync))
                throw new ConfigurationException("A Command Processor with an external bus must have a message mapper registry that implements IAmAMessageMapperRegistryAsync");
            _transformPipelineBuilder = new TransformPipelineBuilder(mapperRegistry, messageTransformerFactory);
            _transformPipelineBuilderAsync = new TransformPipelineBuilderAsync(mapperRegistryAsync, messageTransformerFactoryAsync);

            InitExtServiceBus(bus);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CommandProcessor"/> class.
        /// EXTERNAL BUS, NO INTERNAL BUS: Use this constructor when only posting messages to an external bus is required
        /// </summary>
        /// <param name="requestContextFactory">The request context factory.</param>
        /// <param name="policyRegistry">The policy registry.</param>
        /// <param name="mapperRegistry">The mapper registry.</param>
        /// <param name="bus">The external service bus that we want to send messages over</param>
        /// <param name="featureSwitchRegistry">The feature switch config provider.</param>
        /// <param name="inboxConfiguration">Do we want to insert an inbox handler into pipelines without the attribute. Null (default = no), yes = how to configure</param>
        /// <param name="messageTransformerFactory">The factory used to create a transformer pipeline for a message mapper</param>
        /// <param name="messageTransformerFactoryAsync">The factory used to create a transformer pipeline for a message mapper<</param>
        /// <param name="replySubscriptions">The Subscriptions for creating the reply queues</param>
        public CommandProcessor(
            IAmARequestContextFactory requestContextFactory,
            IAmAnExternalBusService bus,
            IAmAMessageMapperRegistry mapperRegistry = null,
            IAmAFeatureSwitchRegistry featureSwitchRegistry = null,
            InboxConfiguration inboxConfiguration = null,
            IAmAMessageTransformerFactory messageTransformerFactory = null,
            IAmAMessageTransformerFactoryAsync messageTransformerFactoryAsync = null,
            IEnumerable<Subscription> replySubscriptions = null)
        {
            _requestContextFactory = requestContextFactory;
            _featureSwitchRegistry = featureSwitchRegistry;
            _inboxConfiguration = inboxConfiguration;
            _replySubscriptions = replySubscriptions;

            if (mapperRegistry == null)
                throw new ConfigurationException("A Command Processor with an external bus must have a message mapper registry that implements IAmAMessageMapperRegistry");
            if (!(mapperRegistry is IAmAMessageMapperRegistryAsync mapperRegistryAsync))
                throw new ConfigurationException("A Command Processor with an external bus must have a message mapper registry that implements IAmAMessageMapperRegistryAsync");
            _transformPipelineBuilder = new TransformPipelineBuilder(mapperRegistry, messageTransformerFactory);
            _transformPipelineBuilderAsync = new TransformPipelineBuilderAsync(mapperRegistryAsync, messageTransformerFactoryAsync);

            InitExtServiceBus(bus);
        }


        /// <summary>
        /// Sends the specified command. We expect only one handler. The command is handled synchronously.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="command">The command.</param>
        /// <exception cref="ArgumentException">
        /// </exception>
        public void Send<T>(T command) where T : class, IRequest
        {
            if (_handlerFactorySync == null)
                throw new InvalidOperationException("No handler factory defined.");

            var span = GetSpan(PROCESSCOMMAND);
            command.Span = span.span;

            var requestContext = _requestContextFactory.Create();
            requestContext.FeatureSwitches = _featureSwitchRegistry;

            using (var builder = new PipelineBuilder<T>(_subscriberRegistry, _handlerFactorySync, _inboxConfiguration))
            {
                try
                {
                    var handlerChain = builder.Build(requestContext);

                    AssertValidSendPipeline(command, handlerChain.Count());

                    handlerChain.First().Handle(command);
                }
                catch (Exception)
                {
                    span.span?.SetStatus(ActivityStatusCode.Error);
                    throw;
                }
                finally
                {
                    EndSpan(span.span);
                }
            }
        }

        /// <summary>
        /// Awaitably sends the specified command.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="command">The command.</param>
        /// <param name="continueOnCapturedContext">Should we use the calling thread's synchronization context when continuing or a default thread synchronization context. Defaults to false</param>
        /// <param name="cancellationToken">Allows the sender to cancel the request pipeline. Optional</param>
        /// <returns>awaitable <see cref="Task"/>.</returns>
        public async Task SendAsync<T>(T command, bool continueOnCapturedContext = false, CancellationToken cancellationToken = default)
            where T : class, IRequest
        {
            if (_handlerFactoryAsync == null)
                throw new InvalidOperationException("No async handler factory defined.");

            var span = GetSpan(PROCESSCOMMAND);
            command.Span = span.span;

            var requestContext = _requestContextFactory.Create();
            requestContext.FeatureSwitches = _featureSwitchRegistry;

            using (var builder = new PipelineBuilder<T>(_subscriberRegistry, _handlerFactoryAsync, _inboxConfiguration))
            {
                try
                {
                    var handlerChain = builder.BuildAsync(requestContext, continueOnCapturedContext);

                    AssertValidSendPipeline(command, handlerChain.Count());

                    await handlerChain.First().HandleAsync(command, cancellationToken)
                        .ConfigureAwait(continueOnCapturedContext);
                }
                catch (Exception)
                {
                    span.span?.SetStatus(ActivityStatusCode.Error);
                    throw;
                }
                finally
                {
                    EndSpan(span.span);
                }
            }
        }

        /// <summary>
        /// Publishes the specified event. We expect zero or more handlers. The events are handled synchronously, in turn
        /// Because any pipeline might throw, yet we want to execute the remaining handler chains,  we catch exceptions on any publisher
        /// instead of stopping at the first failure and then we throw an AggregateException if any of the handlers failed, 
        /// with the InnerExceptions property containing the failures.
        /// It is up the implementer of the handler that throws to take steps to make it easy to identify the handler that threw.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="event">The event.</param>
        public void Publish<T>(T @event) where T : class, IRequest
        {
            if (_handlerFactorySync == null)
                throw new InvalidOperationException("No handler factory defined.");

            var span = GetSpan(PROCESSEVENT);
            @event.Span = span.span;

            var requestContext = _requestContextFactory.Create();
            requestContext.FeatureSwitches = _featureSwitchRegistry;

            using (var builder = new PipelineBuilder<T>(_subscriberRegistry, _handlerFactorySync, _inboxConfiguration))
            {
                var handlerChain = builder.Build(requestContext);

                var exceptions = new List<Exception>();
                foreach (var handleRequests in handlerChain)
                {
                    try
                    {
                        handleRequests.Handle(@event);
                    }
                    catch (Exception e)
                    {
                        exceptions.Add(e);
                    }
                }

                if (span.created)
                {
                    if (exceptions.Any())
                        span.span?.SetStatus(ActivityStatusCode.Error);
                    EndSpan(span.span);
                }

                if (exceptions.Any())
                {
                    throw new AggregateException(
                        "Failed to publish to one more handlers successfully, see inner exceptions for details",
                        exceptions);
                }
            }
        }

        /// <summary>
        /// Publishes the specified event with async/await. We expect zero or more handlers. The events are handled synchronously and concurrently
        /// Because any pipeline might throw, yet we want to execute the remaining handler chains,  we catch exceptions on any publisher
        /// instead of stopping at the first failure and then we throw an AggregateException if any of the handlers failed, 
        /// with the InnerExceptions property containing the failures.
        /// It is up the implementer of the handler that throws to take steps to make it easy to identify the handler that threw.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="event">The event.</param>
        /// <param name="continueOnCapturedContext">Should we use the calling thread's synchronization context when continuing or a default thread synchronization context. Defaults to false</param>
        /// <param name="cancellationToken">Allows the sender to cancel the request pipeline. Optional</param>
        /// <returns>awaitable <see cref="Task"/>.</returns>
        public async Task PublishAsync<T>(T @event, bool continueOnCapturedContext = false,
            CancellationToken cancellationToken = default)
            where T : class, IRequest
        {
            if (_handlerFactoryAsync == null)
                throw new InvalidOperationException("No async handler factory defined.");

            var span = GetSpan(PROCESSEVENT);
            @event.Span = span.span;

            var requestContext = _requestContextFactory.Create();
            requestContext.FeatureSwitches = _featureSwitchRegistry;

            using (var builder = new PipelineBuilder<T>(_subscriberRegistry, _handlerFactoryAsync, _inboxConfiguration))
            {
                var handlerChain = builder.BuildAsync(requestContext, continueOnCapturedContext);

                var exceptions = new List<Exception>();
                foreach (var handler in handlerChain)
                {
                    try
                    {
                        await handler.HandleAsync(@event, cancellationToken).ConfigureAwait(continueOnCapturedContext);
                    }
                    catch (Exception e)
                    {
                        exceptions.Add(e);
                    }
                }


                if (span.created)
                {
                    if (exceptions.Any())
                        span.span?.SetStatus(ActivityStatusCode.Error);
                    EndSpan(span.span);
                }

                if (exceptions.Count > 0)
                {
                    throw new AggregateException(
                        "Failed to async publish to one more handlers successfully, see inner exceptions for details",
                        exceptions);
                }
            }
        }

        /// <summary>
        /// Posts the specified request. The message is placed on a task queue and into a outbox for reposting in the event of failure.
        /// You will need to configure a service that reads from the task queue to process the message
        /// Paramore.Brighter.ServiceActivator provides an endpoint for use in a windows service that reads from a queue
        /// and then Sends or Publishes the message to a <see cref="CommandProcessor"/> within that service. The decision to <see cref="Send{T}"/> or <see cref="Publish{T}"/> is based on the
        /// mapper. Your mapper can map to a <see cref="Message"/> with either a <see cref="T:MessageType.MT_COMMAND"/> , which results in a <see cref="Send{T}(T)"/> or a
        /// <see cref="T:MessageType.MT_EVENT"/> which results in a <see cref="Publish{T}(T)"/>
        /// Please note that this call will not participate in any ambient Transactions, if you wish to have the outbox participate in a Transaction please Use Deposit,
        /// and then after you have committed your transaction use ClearOutbox
        /// </summary>
        /// <param name="request">The request.</param>
        /// <typeparam name="TRequest">The type of request</typeparam>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public void Post<TRequest>(TRequest request) where TRequest : class, IRequest
        {
            ClearOutbox(DepositPost(request, (IAmABoxTransactionProvider<CommittableTransaction>)null));
        }

        /// <summary>
        /// Posts the specified request with async/await support. The message is placed on a task queue and into a outbox for reposting in the event of failure.
        /// You will need to configure a service that reads from the task queue to process the message
        /// Paramore.Brighter.ServiceActivator provides an endpoint for use in a windows service that reads from a queue
        /// and then Sends or Publishes the message to a <see cref="CommandProcessor"/> within that service. The decision to <see cref="Send{T}"/> or <see cref="Publish{T}"/> is based on the
        /// mapper. Your mapper can map to a <see cref="Message"/> with either a <see cref="T:MessageType.MT_COMMAND"/> , which results in a <see cref="Send{T}(T)"/> or a
        /// <see cref="T:MessageType.MT_EVENT"/> which results in a <see cref="Publish{T}(T)"/>
        /// Please note that this call will not participate in any ambient Transactions, if you wish to have the outbox participate in a Transaction please Use DepositAsync,
        /// and then after you have committed your transaction use ClearOutboxAsync
        /// </summary>
        /// <param name="request">The request.</param>
        /// <param name="continueOnCapturedContext">Should we use the calling thread's synchronization context when continuing or a default thread synchronization context. Defaults to false</param>
        /// <param name="cancellationToken">Allows the sender to cancel the request pipeline. Optional</param>
        /// <typeparam name="TRequest">The type of request</typeparam>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        /// <returns>awaitable <see cref="Task"/>.</returns>
        public async Task PostAsync<TRequest>(
            TRequest request,
            bool continueOnCapturedContext = false,
            CancellationToken cancellationToken = default
            )
            where TRequest : class, IRequest
        {
            var messageId = await DepositPostAsync(request, (IAmABoxTransactionProvider<CommittableTransaction>)null, continueOnCapturedContext, cancellationToken);
            await ClearOutboxAsync(new Guid[] { messageId }, continueOnCapturedContext, cancellationToken);
        }

        /// <summary>
        /// Adds a message into the outbox, and returns the id of the saved message.
        /// Intended for use with the Outbox pattern: http://gistlabs.com/2014/05/the-outbox/ normally you include the
        /// call to DepositPostBox within the scope of the transaction to write corresponding entity state to your
        /// database, that you want to signal via the request to downstream consumers
        /// Pass deposited Guid to <see cref="ClearOutbox"/> 
        /// </summary>
        /// <param name="request">The request to save to the outbox</param>
        /// <typeparam name="TRequest">The type of the request</typeparam>
        /// <returns>The Id of the Message that has been deposited.</returns>
        public Guid DepositPost<TRequest>(TRequest request) where TRequest : class, IRequest
        {
            return DepositPost<TRequest, CommittableTransaction>(request, null);
        }

        /// <summary>
        /// Adds a message into the outbox, and returns the id of the saved message.
        /// Intended for use with the Outbox pattern: http://gistlabs.com/2014/05/the-outbox/ normally you include the
        /// call to DepositPostBox within the scope of the transaction to write corresponding entity state to your
        /// database, that you want to signal via the request to downstream consumers
        /// Pass deposited Guid to <see cref="ClearOutbox"/> 
        /// </summary>
        /// <param name="request">The request to save to the outbox</param>
        /// <param name="transactionProvider">The transaction provider to use with an outbox</param>
        /// <typeparam name="TRequest">The type of the request</typeparam>
        /// <typeparam name="TTransaction">The type of Db transaction used by the Outbox</typeparam>
        /// <returns>The Id of the Message that has been deposited.</returns>
        public Guid DepositPost<TRequest, TTransaction>(
            TRequest request,
            IAmABoxTransactionProvider<TTransaction> transactionProvider
            ) where TRequest : class, IRequest
        {
            var bus = (ExternalBusServices<Message, TTransaction>)_bus;

            if (!bus.HasOutbox())
                throw new InvalidOperationException("No outbox defined.");

            var message = MapMessage<TRequest, TTransaction>(request, new CancellationToken()).GetAwaiter().GetResult();

            AddTelemetryToMessage<TRequest>(message);

            bus.AddToOutbox(request, message, transactionProvider);

            return message.Id;
        }

        /// <summary>
        /// Adds a messages into the outbox, and returns the id of the saved message.
        /// Intended for use with the Outbox pattern: http://gistlabs.com/2014/05/the-outbox/ normally you include the
        /// call to DepositPostBox within the scope of the transaction to write corresponding entity state to your
        /// database, that you want to signal via the request to downstream consumers
        /// Pass deposited Guid to <see cref="ClearOutbox"/> 
        /// </summary>
        /// <param name="requests">The requests to save to the outbox</param>
        /// <typeparam name="TRequest">The type of the request</typeparam>
        /// <returns>The Id of the Message that has been deposited.</returns>
        public Guid[] DepositPost<TRequest>(IEnumerable<TRequest> requests)
            where TRequest : class, IRequest
        {
            return DepositPost<TRequest, CommittableTransaction>(requests, null);
        }

        /// <summary>
        /// Adds a messages into the outbox, and returns the id of the saved message.
        /// Intended for use with the Outbox pattern: http://gistlabs.com/2014/05/the-outbox/ normally you include the
        /// call to DepositPostBox within the scope of the transaction to write corresponding entity state to your
        /// database, that you want to signal via the request to downstream consumers
        /// Pass deposited Guid to <see cref="ClearOutbox"/> 
        /// </summary>
        /// <param name="requests">The requests to save to the outbox</param>
        /// <param name="transactionProvider">The transaction provider to use with an outbox</param>
        /// <typeparam name="TRequest">The type of the request</typeparam>
        /// <typeparam name="TTransaction">The type of transaction used by the Outbox</typeparam>
        /// <returns>The Id of the Message that has been deposited.</returns>
        public Guid[] DepositPost<TRequest, TTransaction>(
            IEnumerable<TRequest> requests,
            IAmABoxTransactionProvider<TTransaction> transactionProvider
            ) where TRequest : class, IRequest
        {
            var bus = (ExternalBusServices<Message, TTransaction>)_bus;

            if (!bus.HasBulkOutbox())
                throw new InvalidOperationException("No Bulk outbox defined.");

            var successfullySentMessage = new List<Guid>();

            foreach (var batch in SplitRequestBatchIntoTypes(requests))
            {
                var messages = MapMessages(batch.Key, batch, new CancellationToken()).GetAwaiter().GetResult();

                bus.AddToOutbox(messages, transactionProvider);

                successfullySentMessage.AddRange(messages.Select(m => m.Id));
            }

            return successfullySentMessage.ToArray();
        }

        /// <summary>
        /// Adds a message into the outbox, and returns the id of the saved message.
        /// Intended for use with the Outbox pattern: http://gistlabs.com/2014/05/the-outbox/ normally you include the
        /// call to DepositPostBox within the scope of the transaction to write corresponding entity state to your
        /// database, that you want to signal via the request to downstream consumers
        /// Pass deposited Guid to <see cref="ClearOutboxAsync"/>
        /// NOTE: If you get an error about the transaction type not matching CommittableTransaction, then you need to
        /// use the specialized version of this method that takes a transaction provider.
        /// </summary>
        /// <param name="request">The request to save to the outbox</param>
        /// <param name="continueOnCapturedContext">Should we use the calling thread's synchronization context when continuing or a default thread synchronization context. Defaults to false</param>
        /// <param name="cancellationToken">The Cancellation Token.</param>
        /// <typeparam name="TRequest">The type of the request</typeparam>
        /// <returns></returns>
        public async Task<Guid> DepositPostAsync<TRequest>(
            TRequest request,
            bool continueOnCapturedContext = false,
            CancellationToken cancellationToken = default
        ) where TRequest : class, IRequest
        {
            return await DepositPostAsync<TRequest, CommittableTransaction>(
                request,
                null,
                continueOnCapturedContext,
                cancellationToken);
        }

        /// <summary>
        /// Adds a message into the outbox, and returns the id of the saved message.
        /// Intended for use with the Outbox pattern: http://gistlabs.com/2014/05/the-outbox/ normally you include the
        /// call to DepositPostBox within the scope of the transaction to write corresponding entity state to your
        /// database, that you want to signal via the request to downstream consumers
        /// Pass deposited Guid to <see cref="ClearOutboxAsync"/> 
        /// </summary>
        /// <param name="request">The request to save to the outbox</param>
        /// <param name="transactionProvider">The transaction provider to use with an outbox</param>
        /// <param name="continueOnCapturedContext">Should we use the calling thread's synchronization context when continuing or a default thread synchronization context. Defaults to false</param>
        /// <param name="cancellationToken">The Cancellation Token.</param>
        /// <typeparam name="TRequest">The type of the request</typeparam>
        /// <typeparam name="TTransaction">The type of the transaction used by the Outbox</typeparam>
        /// <returns></returns>
        public async Task<Guid> DepositPostAsync<TRequest, TTransaction>(
            TRequest request,
            IAmABoxTransactionProvider<TTransaction> transactionProvider,
            bool continueOnCapturedContext = false,
            CancellationToken cancellationToken = default
        ) where TRequest : class, IRequest
        {
            var bus = (ExternalBusServices<Message, TTransaction>)_bus;

            if (!bus.HasAsyncOutbox())
                throw new InvalidOperationException("No async outbox defined.");

            Message message = await MapMessage<TRequest, TTransaction>(request, cancellationToken);

            AddTelemetryToMessage<TRequest>(message);

            await bus.AddToOutboxAsync(request, message,
                transactionProvider, continueOnCapturedContext, cancellationToken);

            return message.Id;
        }



        /// <summary>
        /// Adds a message into the outbox, and returns the id of the saved message.
        /// Intended for use with the Outbox pattern: http://gistlabs.com/2014/05/the-outbox/ normally you include the
        /// call to DepositPostBox within the scope of the transaction to write corresponding entity state to your
        /// database, that you want to signal via the request to downstream consumers
        /// Pass deposited Guid to <see cref="ClearOutboxAsync"/> 
        /// </summary>
        /// <param name="requests">The requests to save to the outbox</param>
        /// <param name="continueOnCapturedContext">Should we use the calling thread's synchronization context when continuing or a default thread synchronization context. Defaults to false</param>
        /// <param name="cancellationToken">The Cancellation Token.</param>
        /// <typeparam name="TRequest">The type of the request</typeparam>
        /// <returns></returns>
        public async Task<Guid[]> DepositPostAsync<TRequest>(
            IEnumerable<TRequest> requests,
            bool continueOnCapturedContext = false,
            CancellationToken cancellationToken = default
            ) where TRequest : class, IRequest
        {
            return await DepositPostAsync<TRequest, CommittableTransaction>(
                requests,
                null,
                continueOnCapturedContext,
                cancellationToken);
        }

        /// <summary>
        /// Adds a message into the outbox, and returns the id of the saved message.
        /// Intended for use with the Outbox pattern: http://gistlabs.com/2014/05/the-outbox/ normally you include the
        /// call to DepositPostBox within the scope of the transaction to write corresponding entity state to your
        /// database, that you want to signal via the request to downstream consumers
        /// Pass deposited Guid to <see cref="ClearOutboxAsync"/> 
        /// </summary>
        /// <param name="requests">The requests to save to the outbox</param>
        /// <param name="transactionProvider">The transaction provider used with the Outbox</param>
        /// <param name="continueOnCapturedContext">Should we use the calling thread's synchronization context when continuing or a default thread synchronization context. Defaults to false</param>
        /// <param name="cancellationToken">The Cancellation Token.</param>
        /// <typeparam name="TRequest">The type of the request</typeparam>
        /// <typeparam name="TTransaction">The type of transaction used with the Outbox</typeparam>
        /// <returns></returns>
        public async Task<Guid[]> DepositPostAsync<TRequest, TTransaction>(
            IEnumerable<TRequest> requests,
            IAmABoxTransactionProvider<TTransaction> transactionProvider,
            bool continueOnCapturedContext = false,
            CancellationToken cancellationToken = default
            ) where TRequest : class, IRequest
        {
            var bus = (ExternalBusServices<Message, TTransaction>)_bus;

            if (!bus.HasAsyncBulkOutbox())
                throw new InvalidOperationException("No bulk async outbox defined.");

            var successfullySentMessage = new List<Guid>();

            foreach (var batch in SplitRequestBatchIntoTypes(requests))
            {
                var messages = await MapMessages(batch.Key, batch.ToArray(), cancellationToken);

                await bus.AddToOutboxAsync(messages, transactionProvider, continueOnCapturedContext, cancellationToken);

                successfullySentMessage.AddRange(messages.Select(m => m.Id));
            }

            return successfullySentMessage.ToArray();
        }

        /// <summary>
        /// Flushes the messages in the id list from the Outbox.
        /// Intended for use with the Outbox pattern: http://gistlabs.com/2014/05/the-outbox/ <see cref="DepositPostBox"/>
        /// </summary>
        /// <param name="ids">The message ids to flush</param>
        public void ClearOutbox(params Guid[] ids)
        {
            _bus.ClearOutbox(ids);
        }

        /// <summary>
        /// Flushes any outstanding message box message to the broker.
        /// This will be run on a background task.
        /// Intended for use with the Outbox pattern: http://gistlabs.com/2014/05/the-outbox/ <see cref="DepositPostBox"/>
        /// </summary>
        /// <param name="amountToClear">The maximum number to clear.</param>
        /// <param name="minimumAge">The minimum age to clear in milliseconds.</param>
        /// <param name="args">Optional bag of arguments required by an outbox implementation to sweep</param>
        public void ClearOutbox(int amountToClear = 100, int minimumAge = 5000, Dictionary<string, object> args = null)
        {
            _bus.ClearOutbox(amountToClear, minimumAge, false, false, args);
        }

        /// <summary>
        /// Flushes the message box message given by <param name="posts"> to the broker.
        /// Intended for use with the Outbox pattern: http://gistlabs.com/2014/05/the-outbox/ <see cref="DepositPostBoxAsync"/>
        /// </summary>
        /// <param name="posts">The ids to flush</param>
        /// <param name="continueOnCapturedContext">Should the callback run on a new thread?</param>
        /// <param name="cancellationToken">The token to cancel a running asynchronous operation</param>
        public async Task ClearOutboxAsync(
            IEnumerable<Guid> posts,
            bool continueOnCapturedContext = false,
            CancellationToken cancellationToken = default)
        {
            await _bus.ClearOutboxAsync(posts, continueOnCapturedContext, cancellationToken);
        }

        /// <summary>
        /// Flushes any outstanding message box message to the broker.
        /// This will be run on a background task.
        /// Intended for use with the Outbox pattern: http://gistlabs.com/2014/05/the-outbox/ <see cref="DepositPostBoxAsync"/>
        /// </summary>
        /// <param name="amountToClear">The maximum number to clear.</param>
        /// <param name="minimumAge">The minimum age to clear in milliseconds.</param>
        /// <param name="useBulk">Use the bulk send on the producer.</param>
        /// <param name="args">Optional bag of arguments required by an outbox implementation to sweep</param>
        public void ClearAsyncOutbox(
            int amountToClear = 100,
            int minimumAge = 5000,
            bool useBulk = false,
            Dictionary<string, object> args = null
        )
        {
            _bus.ClearOutbox(amountToClear, minimumAge, true, useBulk, args);
        }

        /// <summary>
        /// Uses the Request-Reply messaging approach to send a message to another server and block awaiting a reply.
        /// The message is placed into a message queue but not into the outbox.
        /// An ephemeral reply queue is created, and its name used to set the reply address for the response. We produce
        /// a queue per exchange, to simplify correlating send and receive.
        /// The response is directed to a registered handler.
        /// Because the operation blocks, there is a mandatory timeout
        /// </summary>
        /// <param name="request">What message do we want a reply to</param>
        /// <param name="timeOutInMilliseconds">The call blocks, so we must time out</param>
        /// <exception cref="NotImplementedException"></exception>
        public TResponse Call<T, TResponse>(T request, int timeOutInMilliseconds)
            where T : class, ICall where TResponse : class, IResponse
        {
            if (timeOutInMilliseconds <= 0)
            {
                throw new InvalidOperationException("Timeout to a call method must have a duration greater than zero");
            }

            var outWrapPipeline = _transformPipelineBuilder.BuildWrapPipeline<T>();

            var subscription = _replySubscriptions.FirstOrDefault(s => s.DataType == typeof(TResponse));

            if (subscription is null)
                throw new ArgumentOutOfRangeException($"No Subscription registered fpr replies of type {typeof(T)}");

            //create a reply queue via creating a consumer - we use random identifiers as we will destroy
            var channelName = Guid.NewGuid();
            var routingKey = channelName.ToString();

            subscription.ChannelName = new ChannelName(channelName.ToString());
            subscription.RoutingKey = new RoutingKey(routingKey);

            using (var responseChannel = _responseChannelFactory.CreateChannel(subscription))
            {
                request.ReplyAddress.Topic = routingKey;
                request.ReplyAddress.CorrelationId = channelName;

                //we do this to create the channel on the broker, or we won't have anything to send to; we 
                //retry in case the subscription is poor. An alternative would be to extract the code from
                //the channel to create the subscription, but this does not do much on a new queue
                responseChannel.Purge();

                var outMessage = outWrapPipeline.Wrap(request);

                //We don't store the message, if we continue to fail further retry is left to the sender 
                //s_logger.LogDebug("Sending request  with routingkey {0}", routingKey);
                _bus.CallViaExternalBus<T, TResponse>(outMessage);

                Message responseMessage = null;

                //now we block on the receiver to try and get the message, until timeout.
                responseMessage = responseChannel.Receive(timeOutInMilliseconds);

                TResponse response = default;
                if (responseMessage.Header.MessageType != MessageType.MT_NONE)
                {
                    //map to request is map to a response, but it is a request from consumer point of view. Confusing, but...
                    //TODO: this only handles a synchronous unwrap pipeline, we need to handle async too
                    var inUnwrapPipeline = _transformPipelineBuilder.BuildUnwrapPipeline<TResponse>();
                    response = inUnwrapPipeline.Unwrap(responseMessage);
                    Send(response);
                }

                return response;
            } //clean up everything at this point, whatever happens
        }

        /// <summary>
        /// The external service bus is a singleton as it has app lifetime to manage an Outbox.
        /// This method clears the external service bus, so that the next attempt to use it will create a fresh one
        /// It is mainly intended for testing, to allow the external service bus to be reset between tests
        /// </summary>
        public static void ClearExtServiceBus()
        {
            if (_bus != null)
            {
                lock (padlock)
                {
                    if (_bus != null)
                    {
                        _bus.Dispose();
                        _bus = null;
                    }
                }
            }
        }

        private void AddTelemetryToMessage<T>(Message message)
        {
            var activity = Activity.Current ??
                           ApplicationTelemetry.ActivitySource.StartActivity(DEPOSITPOST, ActivityKind.Producer);

            if (activity != null)
            {
                message.Header.AddTelemetryInformation(activity, typeof(T).ToString());
            }
        }

        private void AssertValidSendPipeline<T>(T command, int handlerCount) where T : class, IRequest
        {
            if (handlerCount > 1)
                throw new ArgumentException(
                    $"More than one handler was found for the typeof command {typeof(T)} - a command should only have one handler.");
            if (handlerCount == 0)
                throw new ArgumentException(
                    $"No command handler was found for the typeof command {typeof(T)} - a command should have exactly one handler.");
        }

        private List<Message> BulkMapMessages<T>(IEnumerable<IRequest> requests) where T : class, IRequest
        {
            return requests.Select(r =>
            {
                var wrapPipeline = _transformPipelineBuilder.BuildWrapPipeline<T>();
                var message = wrapPipeline.Wrap((T)r);
                AddTelemetryToMessage<T>(message);
                return message;
            }).ToList();
        }

        private async Task<List<Message>> BulkMapMessagesAsync<T>(IEnumerable<IRequest> requests,
            CancellationToken cancellationToken = default) where T : class, IRequest
        {
            var messages = new List<Message>();
            foreach (var request in requests)
            {
                var wrapPipeline = _transformPipelineBuilderAsync.BuildWrapPipeline<T>();
                var message = await wrapPipeline.WrapAsync((T)request, cancellationToken);
                AddTelemetryToMessage<T>(message);
                messages.Add(message);
            }

            return messages;
        }

        // Create an instance of the ExternalBusServices if one not already set for this app. Note that we do not support reinitialization here, so once you have
        // set a command processor for the app, you can't call init again to set them - although the properties are not read-only so overwriting is possible
        // if needed as a "get out of gaol" card.
        private static void InitExtServiceBus(IAmAnExternalBusService bus)
        {
            if (_bus == null)
            {
                lock (padlock)
                {
                    _bus ??= bus;
                }
            }
        }

        private void EndSpan(Activity span)
        {
            if (span?.Status == ActivityStatusCode.Unset)
                span.SetStatus(ActivityStatusCode.Ok);
            span?.Dispose();
        }

        private (Activity span, bool created) GetSpan(string activityName)
        {
            bool create = Activity.Current == null;

            if (create)
                return (ApplicationTelemetry.ActivitySource.StartActivity(activityName, ActivityKind.Server), create);
            else
                return (Activity.Current, create);
        }

        private bool HandlerFactoryIsNotEitherIAmAHandlerFactorySyncOrAsync(IAmAHandlerFactory handlerFactory)
        {
            // If we do not have a subscriber registry and we do not have a handler factory 
            // then we're creating a control bus sender and we don't need them
            if (_subscriberRegistry is null && handlerFactory is null)
                return false;

            switch (handlerFactory)
            {
                case IAmAHandlerFactorySync _:
                case IAmAHandlerFactoryAsync _:
                    return false;
                default:
                    return true;
            }
        }

        private async Task<Message> MapMessage<TRequest, TTransaction>(TRequest request, CancellationToken cancellationToken)
            where TRequest : class, IRequest
        {
            Message message;
            if (_transformPipelineBuilderAsync.HasPipeline<TRequest>())
            {
                message = await _transformPipelineBuilderAsync
                    .BuildWrapPipeline<TRequest>()
                    .WrapAsync(request, cancellationToken);
            }
            else if (_transformPipelineBuilder.HasPipeline<TRequest>())
            {
                message = _transformPipelineBuilder
                    .BuildWrapPipeline<TRequest>()
                    .Wrap(request);

            }
            else
            {
                throw new ArgumentOutOfRangeException("No message mapper defined for request");
            }

            return message;
        }

        private Task<List<Message>> MapMessages(Type requestType, IEnumerable<IRequest> requests,
            CancellationToken cancellationToken)
        {
            var parameters = new object[] { requests, cancellationToken };

            var hasAsyncPipeline = (bool)typeof(TransformPipelineBuilderAsync)
                    .GetMethod(nameof(TransformPipelineBuilderAsync.HasPipeline),
                        BindingFlags.Instance | BindingFlags.Public)
                .MakeGenericMethod(requestType)
                .Invoke(_transformPipelineBuilderAsync, null);

            if (hasAsyncPipeline)
            {
                return (Task<List<Message>>)GetType()
                    .GetMethod(nameof(BulkMapMessagesAsync), BindingFlags.Instance | BindingFlags.NonPublic)
                    .MakeGenericMethod(requestType)
                    .Invoke(this, parameters);
            }

            var tcs = new TaskCompletionSource<List<Message>>();
            tcs.SetResult((List<Message>)GetType()
                .GetMethod(nameof(BulkMapMessages), BindingFlags.Instance | BindingFlags.NonPublic)
                .MakeGenericMethod(requestType)
                .Invoke(this, new[] { requests }));
            return tcs.Task;
        }

        private IEnumerable<IGrouping<Type, T>> SplitRequestBatchIntoTypes<T>(IEnumerable<T> requests)
        {
            return requests.GroupBy(r => r.GetType());
        }
    }
}
