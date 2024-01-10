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
using FluentAssertions;
using Paramore.Brighter.Core.Tests.CommandProcessors.TestDoubles;
using Paramore.Brighter.Core.Tests.MessageDispatch.TestDoubles;
using Xunit;
using Paramore.Brighter.ServiceActivator;
using Paramore.Brighter.ServiceActivator.TestHelpers;
using System.Text.Json;

namespace Paramore.Brighter.Core.Tests.MessageDispatch
{
    public class MessagePumpCommandRequeueTests
    {
        private readonly IAmAMessagePump _messagePump;
        private readonly FakeChannel _channel;
        private readonly SpyCommandProcessor _commandProcessor;
        private readonly MyCommand _command = new MyCommand();

        public MessagePumpCommandRequeueTests()
        {
            _commandProcessor = new SpyRequeueCommandProcessor();
            var provider = new CommandProcessorProvider(_commandProcessor);
            _channel = new FakeChannel();
            var messageMapperRegistry = new MessageMapperRegistry(
                new SimpleMessageMapperFactory(_ => new MyCommandMessageMapper()),
                null);
             messageMapperRegistry.Register<MyCommand, MyCommandMessageMapper>();
             _messagePump = new MessagePumpBlocking<MyCommand>(provider, messageMapperRegistry, null) 
                { Channel = _channel, TimeoutInMilliseconds = 5000, RequeueCount = -1 };

            var message1 = new Message(new MessageHeader(Guid.NewGuid(), "MyTopic", MessageType.MT_COMMAND), new MessageBody(JsonSerializer.Serialize(_command, JsonSerialisationOptions.Options)));
            var message2 = new Message(new MessageHeader(Guid.NewGuid(), "MyTopic", MessageType.MT_COMMAND), new MessageBody(JsonSerializer.Serialize(_command, JsonSerialisationOptions.Options)));
            _channel.Enqueue(message1);
            _channel.Enqueue(message2);
            var quitMessage = new Message(new MessageHeader(Guid.Empty, "", MessageType.MT_QUIT), new MessageBody(""));
            _channel.Enqueue(quitMessage);
        }

        [Fact]
        public void When_A_Requeue_Of_Command_Exception_Is_Thrown()
        {
            _messagePump.Run();

            //_should_send_the_message_via_the_command_processor
            _commandProcessor.Commands[0].Should().Be(CommandType.Send);
            //_should_requeue_the_messages
            _channel.Length.Should().Be(2);
            //_should_dispose_the_input_channel
            _channel.DisposeHappened.Should().BeTrue();
        }
    }
}
