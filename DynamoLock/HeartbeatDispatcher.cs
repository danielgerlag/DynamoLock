using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Microsoft.Extensions.Logging;

namespace DynamoLock
{
    public class HeartbeatDispatcher : IHeartbeatDispatcher
    {
        private readonly ILogger _logger;
        private readonly IAmazonDynamoDB _client;
        private readonly ILocalLockTracker _lockTracker;
        private readonly string _tableName;

        private TimeSpan _interval;
        private string _nodeId;
        private long _leaseTime;
        private Task _heartbeatTask;
        private CancellationTokenSource _cancellationTokenSource;

        public HeartbeatDispatcher(AWSCredentials credentials, AmazonDynamoDBConfig config, ILocalLockTracker lockTracker, string tableName, ILoggerFactory logFactory)
        {
            _logger = logFactory.CreateLogger<HeartbeatDispatcher>();
            _client = new AmazonDynamoDBClient(credentials, config);
            _tableName = tableName;
            _lockTracker = lockTracker;
        }

        public HeartbeatDispatcher(IAmazonDynamoDB dynamoClient, ILocalLockTracker lockTracker, string tableName, ILoggerFactory logFactory)
        {
            _logger = logFactory.CreateLogger<HeartbeatDispatcher>();
            _client = dynamoClient;
            _tableName = tableName;
            _lockTracker = lockTracker;
        }

        public void Start(string nodeId, TimeSpan interval, long leaseTime)
        {
            _nodeId = nodeId;
            _interval = interval;
            _leaseTime = leaseTime;

            if (_cancellationTokenSource != null)
            {
                _cancellationTokenSource.Cancel();
                _heartbeatTask?.Wait();
            }

            _cancellationTokenSource = new CancellationTokenSource();
            _heartbeatTask = new Task(SendHeartbeat);
            _heartbeatTask.Start();
        }

        public void Stop()
        {
            _cancellationTokenSource?.Cancel();
            _heartbeatTask?.Wait();
        }

        private async void SendHeartbeat()
        {
            while (!_cancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(_interval, _cancellationTokenSource.Token);

                    foreach (var item in _lockTracker.GetSnapshot())
                    {
                        var req = new PutItemRequest
                        {
                            TableName = _tableName,
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "id", new AttributeValue(item) },
                                { "lock_owner", new AttributeValue(_nodeId) },
                                {
                                    "expires", new AttributeValue()
                                    {
                                        N = Convert.ToString(new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds() + _leaseTime)
                                    }
                                },
                                {
                                    "purge_time", new AttributeValue()
                                    {
                                        N = Convert.ToString(new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds() + (_leaseTime * 10))
                                    }
                                }
                            },
                            ConditionExpression = "lock_owner = :node_id",
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                { ":node_id", new AttributeValue(_nodeId) }
                            }
                        };

                        try
                        {
                            await _client.PutItemAsync(req, _cancellationTokenSource.Token);
                        }
                        catch (ConditionalCheckFailedException)
                        {
                            _logger.LogWarning($"Lock not owned anymore when sending heartbeat for {item}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(default(EventId), ex, ex.Message);
                }
            }
        }
    }
}
