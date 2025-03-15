#region Licence
/* The MIT License (MIT)
Copyright Â© 2014 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the â€œSoftwareâ€), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED â€œAS ISâ€, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */

#endregion

using System.Collections.Generic;

namespace ApacheTech.Common.BrighterSlim
{
    /// <summary>
    /// Interface INeedMessaging
    /// Note that a single command builder does not support both task queues and rpc, using the builder
    /// </summary>
    public interface INeedMessaging
    {
        /// <summary>
        /// The <see cref="CommandProcessor"/> wants to support <see cref="CommandProcessor.Post{T}(T)"/> or <see cref="CommandProcessor.Repost"/> using an external bus.
        /// You need to provide a policy to specify how QoS issues, specifically <see cref="CommandProcessor.RETRYPOLICY "/> or <see cref="CommandProcessor.CIRCUITBREAKER "/> 
        /// are handled by adding appropriate <see cref="CommandProcessorBuilder.Policies"/> when choosing this option.
        /// </summary>
        /// <param name="busType">The type of Bus: In-memory, Db, or RPC</param>
        /// <param name="bus">The bus that we wish to use</param>
        /// <param name="messageMapperRegistry">The register for message mappers that map outgoing requests to messages</param>
        /// <param name="transformerFactory">A factory for transforms used for common transformations to outgoing messages</param>
        /// <param name="responseChannelFactory">If using RPC the factory for reply channels</param>
        /// <param name="subscriptions">If using RPC, any reply subscriptions</param>
        /// <param name="inboxConfiguration">What is the inbox configuration</param>
        /// <returns></returns>
        INeedARequestContext ExternalBus(
            ExternalBusType busType,
            IAmAnExternalBusService bus,
            IAmAMessageMapperRegistry messageMapperRegistry,
            IAmAMessageTransformerFactory transformerFactory,
            IAmAChannelFactory responseChannelFactory = null,
            IEnumerable<Subscription> subscriptions = null,
            InboxConfiguration inboxConfiguration = null
            );

        /// <summary>
        /// We don't send messages out of process
        /// </summary>
        /// <returns>INeedARequestContext.</returns>
        INeedARequestContext NoExternalBus();

        /// <summary>
        /// The <see cref="CommandProcessor"/> wants to support <see cref="CommandProcessor.Post{T}(T)"/> or <see cref="CommandProcessor.Repost"/> using an external bus.
        /// You need to provide a policy to specify how QoS issues, specifically <see cref="CommandProcessor.RETRYPOLICY "/> or <see cref="CommandProcessor.CIRCUITBREAKER "/> 
        /// are handled by adding appropriate <see cref="CommandProcessorBuilder.Policies"/> when choosing this option.
        /// 
        /// </summary>
        /// <param name="configuration">The Task Queues configuration.</param>
        /// <param name="outbox">The Outbox.</param>
        /// <param name="transactionProvider"></param>
        /// <returns>INeedARequestContext.</returns>
        INeedARequestContext ExternalBusCreate<TTransaction>(
            ExternalBusConfiguration configuration,
            IAmAnOutbox outbox,
            IAmABoxTransactionProvider<TTransaction> transactionProvider);
    }
}
