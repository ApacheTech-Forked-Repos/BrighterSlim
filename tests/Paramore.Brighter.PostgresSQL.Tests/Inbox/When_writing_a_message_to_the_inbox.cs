﻿#region Licence

/* The MIT License (MIT)
Copyright © 2020 Ian Cooper <ian.cooper@yahoo.co.uk>

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
using Paramore.Brighter.Inbox.Exceptions;
using Paramore.Brighter.Inbox.Postgres;
using Paramore.Brighter.PostgresSQL.Tests.TestDoubles;
using Xunit;

namespace Paramore.Brighter.PostgresSQL.Tests.Inbox
{
    [Trait("Category", "PostgresSql")]
    public class SqlInboxAddMessageTests : IDisposable
    {
        private readonly PostgresSqlTestHelper _pgTestHelper;
        private readonly PostgresSqlInbox _pgSqlInbox;
        private readonly MyCommand _raisedCommand;
        private readonly string _contextKey;
        private MyCommand _storedCommand;

        public SqlInboxAddMessageTests()
        {
            _pgTestHelper = new PostgresSqlTestHelper();
            _pgTestHelper.SetupCommandDb();

            _pgSqlInbox = new PostgresSqlInbox(_pgTestHelper.InboxConfiguration);
            _raisedCommand = new MyCommand { Value = "Test" };
            _contextKey = "context-key";
            _pgSqlInbox.Add(_raisedCommand, _contextKey);
        }

        [Fact]
        public void When_Writing_A_Message_To_The_Inbox()
        {
            _storedCommand = _pgSqlInbox.Get<MyCommand>(_raisedCommand.Id, _contextKey);

            //_should_read_the_command_from_the__sql_inbox
            AssertionExtensions.Should(_storedCommand).NotBeNull();
            //_should_read_the_command_value
            AssertionExtensions.Should(_storedCommand.Value).Be(_raisedCommand.Value);
            //_should_read_the_command_id
            AssertionExtensions.Should(_storedCommand.Id).Be(_raisedCommand.Id);
        }

        [Fact]
        public void When_Reading_A_Message_From_The_Inbox_And_ContextKey_IsNull()
        {
            var exception = Catch.Exception(() => _storedCommand = _pgSqlInbox.Get<MyCommand>(_raisedCommand.Id, null));
            //should_not_read_message
            AssertionExtensions.Should(exception).BeOfType<RequestNotFoundException<MyCommand>>();
        }


        public void Dispose()
        {
            _pgTestHelper.CleanUpDb();
        }
    }
}
