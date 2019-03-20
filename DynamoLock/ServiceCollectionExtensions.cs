using System;
using System.Collections.Generic;
using System.Text;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DynamoLock
{
    public static class ServiceCollectionExtensions
    {
        public static void AddDynamoLockManager(this IServiceCollection serviceCollection, AWSCredentials credentials, AmazonDynamoDBConfig config, string tableName)
        {
            serviceCollection.AddSingleton<ILocalLockTracker, LocalLockTracker>();
            serviceCollection.AddSingleton<IHeartbeatDispatcher>(sp => new HeartbeatDispatcher(credentials, config, sp.GetService<ILocalLockTracker>(), tableName, sp.GetService<ILoggerFactory>() ?? new NullLoggerFactory()));
            serviceCollection.AddSingleton<ILockTableProvisioner>(sp => new LockTableProvisioner(credentials, config, tableName, sp.GetService<ILoggerFactory>() ?? new NullLoggerFactory()));
            serviceCollection.AddSingleton<IDistributedLockManager>(sp => new DynamoDbLockManager(credentials, config, tableName, sp.GetService<ILockTableProvisioner>(), sp.GetService<IHeartbeatDispatcher>(), sp.GetService<ILocalLockTracker>(), sp.GetService<ILoggerFactory>() ?? new NullLoggerFactory()));
        }
    }
}
