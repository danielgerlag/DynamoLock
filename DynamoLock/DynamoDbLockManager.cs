using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DynamoLock
{
    public class DynamoDbLockManager : IDistributedLockManager
    {
        private readonly ILogger _logger;
        private readonly IAmazonDynamoDB _client;
        private readonly ILockTableProvisioner _provisioner;
        private readonly IHeartbeatDispatcher _heartbeatDispatcher;
        private readonly ILocalLockTracker _lockTracker;
        private readonly string _tableName;
        private readonly string _nodeId = Guid.NewGuid().ToString();
        private readonly long _defaultLeaseTime = 30;
        private readonly TimeSpan _heartbeat = TimeSpan.FromSeconds(10);
        private readonly long _jitterTolerance = 1;
        

        public DynamoDbLockManager(AWSCredentials credentials, AmazonDynamoDBConfig config, string tableName, ILockTableProvisioner provisioner, IHeartbeatDispatcher heartbeatDispatcher, ILocalLockTracker lockTracker, ILoggerFactory logFactory)
        {
            _logger = logFactory.CreateLogger<DynamoDbLockManager>();
            _client = new AmazonDynamoDBClient(credentials, config);
            _tableName = tableName;
            _provisioner = provisioner;
            _heartbeatDispatcher = heartbeatDispatcher;
            _lockTracker = lockTracker;
        }

        public DynamoDbLockManager(AWSCredentials credentials, RegionEndpoint region, string tableName, ILoggerFactory logFactory)
        {
            _logger = logFactory.CreateLogger<DynamoDbLockManager>();
            _client = new AmazonDynamoDBClient(credentials, region);
            _tableName = tableName;
            _lockTracker = new LocalLockTracker();
            _provisioner = new LockTableProvisioner(credentials, new AmazonDynamoDBConfig() { RegionEndpoint = region }, tableName, logFactory);
            _heartbeatDispatcher = new HeartbeatDispatcher(credentials, new AmazonDynamoDBConfig() {RegionEndpoint = region}, _lockTracker, tableName, logFactory);
        }

        public DynamoDbLockManager(IAmazonDynamoDB dynamoClient, string tableName, ILoggerFactory logFactory)
        {
            _logger = logFactory.CreateLogger<DynamoDbLockManager>();
            _client = dynamoClient;
            _tableName = tableName;
            _lockTracker = new LocalLockTracker();
            _provisioner = new LockTableProvisioner(dynamoClient, tableName, logFactory);
            _heartbeatDispatcher = new HeartbeatDispatcher(dynamoClient, _lockTracker, tableName, logFactory);
        }

        public DynamoDbLockManager(AWSCredentials credentials, AmazonDynamoDBConfig config, string tableName, ILockTableProvisioner provisioner, IHeartbeatDispatcher heartbeatDispatcher, ILocalLockTracker lockTracker, ILoggerFactory logFactory, long defaultLeaseTime, TimeSpan hearbeat)
            : this(credentials, config, tableName, provisioner, heartbeatDispatcher, lockTracker, logFactory)
        {
            _defaultLeaseTime = defaultLeaseTime;
            _heartbeat = hearbeat;
        }

        public async Task<bool> AcquireLock(string Id)
        {
            try
            {
                var req = new PutItemRequest()
                {
                    TableName = _tableName,
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { "id", new AttributeValue(Id) },
                        { "lock_owner", new AttributeValue(_nodeId) },
                        {
                            "expires", new AttributeValue()
                            {
                                N = Convert.ToString(new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds() + _defaultLeaseTime)
                            }
                        },
                        {
                            "purge_time", new AttributeValue()
                            {
                                N = Convert.ToString(new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds() + (_defaultLeaseTime * 10))
                            }
                        }
                    },
                    ConditionExpression = "attribute_not_exists(id) OR (expires < :expired)",
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { ":expired", new AttributeValue()
                            {
                                N = Convert.ToString(new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds() + _jitterTolerance)
                            }
                        }
                    }
                };

                var response = await _client.PutItemAsync(req);

                if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
                {
                    _lockTracker.Add(Id);
                    return true;
                }
            }
            catch (ConditionalCheckFailedException)
            {
            }
            return false;
        }

        public async Task ReleaseLock(string Id)
        {
            _lockTracker.Remove(Id);
        
            try
            {
                var req = new DeleteItemRequest()
                {
                    TableName = _tableName,
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "id", new AttributeValue(Id) }
                    },
                    ConditionExpression = "lock_owner = :node_id",
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { ":node_id", new AttributeValue(_nodeId) }
                    }

                };
                await _client.DeleteItemAsync(req);
            }
            catch (ConditionalCheckFailedException)
            {
            }
        }

        public async Task Start()
        {
            await _provisioner.Provision();
            _heartbeatDispatcher.Start(_nodeId, _heartbeat, _defaultLeaseTime);
        }

        public Task Stop()
        {
            _heartbeatDispatcher.Stop();
            _lockTracker.Clear();
            return Task.CompletedTask;
        }
    }
}
