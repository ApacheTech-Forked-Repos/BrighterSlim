﻿using System;
using System.IO;
using System.Threading.Tasks;
using FluentMigrator.Runner;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.Inbox;
using Paramore.Brighter.Inbox.MySql;
using Paramore.Brighter.Inbox.Sqlite;
using Paramore.Brighter.MessagingGateway.RMQ;
using Paramore.Brighter.MySql;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Paramore.Brighter.Sqlite;
using SalutationAnalytics.Database;
using SalutationPorts.Policies;
using SalutationPorts.Requests;

namespace SalutationAnalytics
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            var host = CreateHostBuilder(args).Build();
            host.CheckDbIsUp();
            host.MigrateDatabase();
            host.CreateInbox();
            host.CreateOutbox();
            await host.RunAsync();
        }

        private static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureHostConfiguration(configurationBuilder =>
                {
                    configurationBuilder.SetBasePath(Directory.GetCurrentDirectory());
                    configurationBuilder.AddJsonFile("appsettings.json", optional: true);
                    configurationBuilder.AddJsonFile($"appsettings.{GetEnvironment()}.json", optional: true);
                    configurationBuilder.AddEnvironmentVariables(prefix: "ASPNETCORE_"); //NOTE: Although not web, we use this to grab the environment
                    configurationBuilder.AddEnvironmentVariables(prefix: "BRIGHTER_");
                    configurationBuilder.AddCommandLine(args);
                })
                .ConfigureLogging((context, builder) =>
                {
                    builder.AddConsole();
                    builder.AddDebug();
                })
                .ConfigureServices((hostContext, services) =>
                {
                    ConfigureMigration(hostContext, services);
                    ConfigureDapper(hostContext, services);
                    ConfigureBrighter(hostContext, services);
                })
                .UseConsoleLifetime();

        private static void ConfigureBrighter(HostBuilderContext hostContext, IServiceCollection services)
        {
            var subscriptions = new Subscription[]
            {
                new RmqSubscription<GreetingMade>(
                    new SubscriptionName("paramore.sample.salutationanalytics"),
                    new ChannelName("SalutationAnalytics"),
                    new RoutingKey("GreetingMade"),
                    runAsync: false,
                    timeoutInMilliseconds: 200,
                    isDurable: true,
                    makeChannels: OnMissingChannel.Create), //change to OnMissingChannel.Validate if you have infrastructure declared elsewhere
            };

            var relationalDatabaseConfiguration =
                new RelationalDatabaseConfiguration(DbConnectionString(hostContext), SchemaCreation.INBOX_TABLE_NAME);
            services.AddSingleton<IAmARelationalDatabaseConfiguration>(relationalDatabaseConfiguration);
            
            var rmqConnection = new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(new Uri($"amqp://guest:guest@localhost:5672")), Exchange = new Exchange("paramore.brighter.exchange")
            };

            var rmqMessageConsumerFactory = new RmqMessageConsumerFactory(rmqConnection);

            var producerRegistry = new RmqProducerRegistryFactory(
                rmqConnection,
                new RmqPublication[]
                {
                    new RmqPublication
                    {
                        Topic = new RoutingKey("SalutationReceived"),
                        MaxOutStandingMessages = 5,
                        MaxOutStandingCheckIntervalMilliSeconds = 500,
                        WaitForConfirmsTimeOutInMilliseconds = 1000,
                        MakeChannels = OnMissingChannel.Create
                    }
                }
            ).Create();

            services.AddServiceActivator(options =>
                {
                    options.Subscriptions = subscriptions;
                    options.ChannelFactory = new ChannelFactory(rmqMessageConsumerFactory);
                    options.UseScoped = true;
                    options.HandlerLifetime = ServiceLifetime.Scoped;
                    options.MapperLifetime = ServiceLifetime.Singleton;
                    options.CommandProcessorLifetime = ServiceLifetime.Scoped;
                    options.PolicyRegistry = new SalutationPolicy();
                    options.InboxConfiguration = new InboxConfiguration(
                        CreateInbox(hostContext, relationalDatabaseConfiguration),
                        scope: InboxScope.Commands,
                        onceOnly: true,
                        actionOnExists: OnceOnlyAction.Throw

                    );
                })
                .ConfigureJsonSerialisation((options) =>
                {
                    //We don't strictly need this, but added as an example
                    options.PropertyNameCaseInsensitive = true;
                })
                .UseExternalBus((config) =>
                {
                    config.ProducerRegistry = producerRegistry;
                })
                .AutoFromAssemblies();

            services.AddHostedService<ServiceActivatorHostedService>();
        }

        private static void ConfigureMigration(HostBuilderContext hostBuilderContext, IServiceCollection services)
        {
            //dev is always Sqlite
            if (hostBuilderContext.HostingEnvironment.IsDevelopment())
                ConfigureSqlite(hostBuilderContext, services);
            else
                ConfigureProductionDatabase(hostBuilderContext, GetDatabaseType(hostBuilderContext), services);
        }

        private static void ConfigureProductionDatabase(
            HostBuilderContext hostBuilderContext, 
            DatabaseType databaseType,
            IServiceCollection services)
        {
            switch (databaseType)
            {
                case DatabaseType.MySql:
                    ConfigureMySql(hostBuilderContext, services);
                    break;
                case DatabaseType.MsSql:
                    ConfigureMsSql(hostBuilderContext, services);
                    break;
                case DatabaseType.Postgres:
                    ConfigurePostgreSql(hostBuilderContext, services);
                    break;
                case DatabaseType.Sqlite:
                    ConfigureSqlite(hostBuilderContext, services);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(databaseType), "Database type is not supported");
            }
        }
        
        private static void ConfigureMySql(HostBuilderContext hostBuilderContext, IServiceCollection services)
        {
            services
                .AddFluentMigratorCore()
                .ConfigureRunner(c => c.AddMySql5()
                    .WithGlobalConnectionString(DbConnectionString(hostBuilderContext))
                    .ScanIn(typeof(Salutations_Migrations.Migrations.SqlInitialMigrations).Assembly).For.Migrations()
                );
        }
        
        private static void ConfigureMsSql(HostBuilderContext hostBuilderContext, IServiceCollection services)
        {
            services
                .AddFluentMigratorCore()
                .ConfigureRunner(c => c.AddSqlServer()
                    .WithGlobalConnectionString(DbConnectionString(hostBuilderContext))
                    .ScanIn(typeof(Salutations_Migrations.Migrations.SqlInitialMigrations).Assembly).For.Migrations()
                );
        }
        
        private static void ConfigurePostgreSql(HostBuilderContext hostBuilderContext, IServiceCollection services)
        {
            services
                .AddFluentMigratorCore()
                .ConfigureRunner(c => c.AddPostgres()
                    .WithGlobalConnectionString(DbConnectionString(hostBuilderContext))
                    .ScanIn(typeof(Salutations_Migrations.Migrations.SqlInitialMigrations).Assembly).For.Migrations()
                );
        }

        private static void ConfigureSqlite(HostBuilderContext hostBuilderContext, IServiceCollection services)
        {
            services
                .AddFluentMigratorCore()
                .ConfigureRunner(c =>
                {
                    c.AddSQLite()
                        .WithGlobalConnectionString(DbConnectionString(hostBuilderContext))
                        .ScanIn(typeof(Salutations_Migrations.Migrations.SqlInitialMigrations).Assembly).For.Migrations();
                });
        }

        private static void ConfigureDapper(HostBuilderContext hostBuilderContext, IServiceCollection services)
        {
            ConfigureDapperByHost(GetDatabaseType(hostBuilderContext), services);
        }

        private static void ConfigureDapperByHost(DatabaseType databaseType, IServiceCollection services)
        {
            switch (databaseType)
            {
                case DatabaseType.Sqlite:
                    ConfigureDapperSqlite(services);
                    break;
                case DatabaseType.MySql:
                    ConfigureDapperMySql(services);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(databaseType), "Database type is not supported");
            }
        }

        private static void ConfigureDapperSqlite(IServiceCollection services)
        {
            services.AddScoped<IAmARelationalDbConnectionProvider, SqliteConnectionProvider>();
            services.AddScoped<IAmATransactionConnectionProvider, SqliteUnitOfWork>();
        }

        private static void ConfigureDapperMySql(IServiceCollection services)
        {
            services.AddScoped<IAmARelationalDbConnectionProvider, MySqlConnectionProvider>();
            services.AddScoped<IAmATransactionConnectionProvider, MySqlUnitOfWork>();
        }


        private static IAmAnInbox CreateInbox(HostBuilderContext hostContext, IAmARelationalDatabaseConfiguration configuration)
        {
            if (hostContext.HostingEnvironment.IsDevelopment())
            {
                return new SqliteInbox(configuration);
            }

            return new MySqlInbox(configuration);
        }

        private static string DbConnectionString(HostBuilderContext hostContext)
        {
            //NOTE: Sqlite needs to use a shared cache to allow Db writes to the Outbox as well as entities
            return hostContext.HostingEnvironment.IsDevelopment()
                ? "Filename=Salutations.db;Cache=Shared"
                : hostContext.Configuration.GetConnectionString("Salutations");
        }

        private static DatabaseType GetDatabaseType(HostBuilderContext hostContext)
        {
            return hostContext.Configuration[DatabaseGlobals.DATABASE_TYPE_ENV] switch

            {
                DatabaseGlobals.MYSQL => DatabaseType.MySql,
                DatabaseGlobals.MSSQL => DatabaseType.MsSql,
                DatabaseGlobals.POSTGRESSQL => DatabaseType.Postgres,
                DatabaseGlobals.SQLITE => DatabaseType.Sqlite,
                _ => throw new InvalidOperationException("Could not determine the database type")
            };
        }
        
        private static string GetEnvironment()
        {
            //NOTE: Hosting Context will always return Production outside of ASPNET_CORE at this point, so grab it directly
            return Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
        }
    }
}
