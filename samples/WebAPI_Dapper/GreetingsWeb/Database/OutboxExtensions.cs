﻿using System;
using GreetingsEntities;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Npgsql.NameTranslation;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.Extensions.Hosting;
using Paramore.Brighter.MsSql;
using Paramore.Brighter.MySql;
using Paramore.Brighter.Outbox.MsSql;
using Paramore.Brighter.Outbox.MySql;
using Paramore.Brighter.Outbox.PostgreSql;
using Paramore.Brighter.Outbox.Sqlite;
using Paramore.Brighter.PostgreSql;
using Paramore.Brighter.Sqlite;

namespace GreetingsWeb.Database
{

    public class OutboxExtensions
    {
        public static (IAmAnOutbox, Type, Type) MakeOutbox(
            IWebHostEnvironment env,
            DatabaseType databaseType,
            RelationalDatabaseConfiguration configuration,
            IServiceCollection services)
        {
            (IAmAnOutbox, Type, Type) outbox;
            if (env.IsDevelopment())
            {
                outbox = MakeSqliteOutBox(configuration);
            }
            else
            {
                outbox = databaseType switch
                {
                    DatabaseType.MySql => MakeMySqlOutbox(configuration),
                    DatabaseType.MsSql => MakeMsSqlOutbox(configuration),
                    DatabaseType.Postgres => MakePostgresSqlOutbox(configuration, services),
                    DatabaseType.Sqlite => MakeSqliteOutBox(configuration),
                    _ => throw new InvalidOperationException("Unknown Db type for Outbox configuration")
                };
            }

            return outbox;
        }

        private static (IAmAnOutbox, Type, Type) MakePostgresSqlOutbox(
            RelationalDatabaseConfiguration configuration,
            IServiceCollection services)
        {
            //if we want to use our IAmARelationalDatabaseConnectionProvider or IAmAABoxTransactionProvider<DbTransaction>
            //from the Outbox in our handlers, then we need to construct an NpgsqlDataSource and register the composite types
            //then pass that to the Outbox constructor so that connections created by the Outbox will be aware of
            //those composite types
            //var dataSourceBuilder = new NpgsqlDataSourceBuilder(configuration.ConnectionString);
            //dataSourceBuilder.DefaultNameTranslator = new NpgsqlNullNameTranslator();
            //dataSourceBuilder.MapComposite<Person>();
            //dataSourceBuilder.MapComposite<Greeting>();
            //var dataSource = dataSourceBuilder.Build();

            //services.AddSingleton(dataSource);
            
            return (new PostgreSqlOutbox(configuration), typeof(NpgsqConnectionProvider), typeof(NpgsqlUnitOfWork));
        }

        private static (IAmAnOutbox, Type, Type) MakeMsSqlOutbox(RelationalDatabaseConfiguration configuration)
        {
            return new(new MsSqlOutbox(configuration), typeof(MsSqlAuthConnectionProvider), typeof(MsSqlUnitOfWork));
        }

        private static (IAmAnOutbox, Type, Type)  MakeMySqlOutbox(RelationalDatabaseConfiguration configuration)
        {
            return (new MySqlOutbox(configuration), typeof (MySqlConnectionProvider), typeof(MySqlUnitOfWork));
        }

        private static (IAmAnOutbox, Type, Type) MakeSqliteOutBox(RelationalDatabaseConfiguration configuration)
        {
            return (new SqliteOutbox(configuration), typeof(SqliteConnectionProvider), typeof(SqliteUnitOfWork));
        }
    }
}
