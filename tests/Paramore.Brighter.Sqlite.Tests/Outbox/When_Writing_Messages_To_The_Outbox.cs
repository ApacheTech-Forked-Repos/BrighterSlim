#region Licence

/* The MIT License (MIT)
Copyright © 2014 Francesco Pighi <francesco.pighi@gmail.com>

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
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Paramore.Brighter.Outbox.Sqlite;
using Xunit;

namespace Paramore.Brighter.Sqlite.Tests.Outbox
{
    [Trait("Category", "Sqlite")]
    public class SqlOutboxWritngMessagesTests : IAsyncDisposable 
    {
        private readonly SqliteTestHelper _sqliteTestHelper;
        private readonly SqliteOutbox _sqlOutbox;
        private readonly Message _messageOne;
        private readonly Message _messageTwo;
        private readonly Message _messageThree;
        private IEnumerable<Message> _retrievedMessages;

        public SqlOutboxWritngMessagesTests()
        {
            _sqliteTestHelper = new SqliteTestHelper();
            _sqliteTestHelper.SetupMessageDb();
            _sqlOutbox = new SqliteOutbox(_sqliteTestHelper.OutboxConfiguration);
 
            _messageOne = new Message(new MessageHeader(Guid.NewGuid(), "Test", MessageType.MT_COMMAND, DateTime.UtcNow.AddHours(-3)), new MessageBody("Body"));
            _messageTwo = new Message(new MessageHeader(Guid.NewGuid(), "Test2", MessageType.MT_COMMAND, DateTime.UtcNow.AddHours(-2)), new MessageBody("Body2"));
            _messageThree = new Message(new MessageHeader(Guid.NewGuid(), "Test3", MessageType.MT_COMMAND, DateTime.UtcNow.AddHours(-1)), new MessageBody("Body3"));
        }

        [Fact]
        public void When_Writing_Messages_To_The_Outbox()
        {
            _sqlOutbox.Add(_messageOne);
            _sqlOutbox.Add(_messageTwo);
            _sqlOutbox.Add(_messageThree);

            _retrievedMessages = _sqlOutbox.Get();
            

            //should read last message last from the outbox
            _retrievedMessages.Last().Id.Should().Be(_messageThree.Id);
            //should read first message first from the outbox
            _retrievedMessages.First().Id.Should().Be(_messageOne.Id);
            //should read the messages from the outbox
            _retrievedMessages.Should().HaveCount(3);
        }
        
        [Fact]
        public void When_Writing_Messages_To_The_Outbox_Bulk()
        {
            _sqlOutbox.Add(new List<Message> {_messageOne, _messageTwo, _messageThree});

            _retrievedMessages = _sqlOutbox.Get();
            
            //should read first message last from the outbox
            _retrievedMessages.Last().Id.Should().Be(_messageThree.Id);
            //should read last message first from the outbox
            _retrievedMessages.First().Id.Should().Be(_messageOne.Id);
            //should read the messages from the outbox
            _retrievedMessages.Should().HaveCount(3);
        }

        public async ValueTask DisposeAsync()
        {
            await _sqliteTestHelper.CleanUpDbAsync();
        }
    }
}
