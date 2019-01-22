using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Microsoft.Extensions.Logging;

namespace DynamoLock
{
    public class LockTableProvisioner : ILockTableProvisioner
    {
        private readonly ILogger _logger;
        private readonly IAmazonDynamoDB _client;
        private readonly string _tableName;

        public LockTableProvisioner(AWSCredentials credentials, AmazonDynamoDBConfig config, string tableName, ILoggerFactory logFactory)
        {
            _logger = logFactory.CreateLogger<LockTableProvisioner>();
            _client = new AmazonDynamoDBClient(credentials, config);
            _tableName = tableName;
        }

        public LockTableProvisioner(IAmazonDynamoDB dynamoClient, string tableName, ILoggerFactory logFactory)
        {
            _logger = logFactory.CreateLogger<LockTableProvisioner>();
            _client = dynamoClient;
            _tableName = tableName;
        }

        public async Task Provision()
        {
            try
            {
                var poll = await _client.DescribeTableAsync(_tableName);
            }
            catch (ResourceNotFoundException)
            {
                _logger.LogInformation($"Creating lock table {_tableName}");
                await CreateTable();
                await SetPurgeTTL();
            }
        }

        private async Task CreateTable()
        {
            var createRequest = new CreateTableRequest(_tableName, new List<KeySchemaElement>()
            {
                new KeySchemaElement("id", KeyType.HASH)
            })
            {
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition("id", ScalarAttributeType.S)
                },
                ProvisionedThroughput = new ProvisionedThroughput()
                {
                    ReadCapacityUnits = 1,
                    WriteCapacityUnits = 1
                }
                //BillingMode = BillingMode.PAY_PER_REQUEST
            };

            var createResponse = await _client.CreateTableAsync(createRequest);
            
            int i = 0;
            bool created = false;
            while ((i < 20) && (!created))
            {
                try
                {
                    await Task.Delay(1000);
                    var poll = await _client.DescribeTableAsync(_tableName);
                    created = (poll.Table.TableStatus == TableStatus.ACTIVE);
                    i++;
                }
                catch (ResourceNotFoundException)
                {
                }
            }
        }

        private async Task SetPurgeTTL()
        {
            var request = new UpdateTimeToLiveRequest()
            {
                TableName = _tableName,
                TimeToLiveSpecification = new TimeToLiveSpecification()
                {
                    AttributeName = "purge_time",
                    Enabled = true
                }
            };

            await _client.UpdateTimeToLiveAsync(request);
        }
    }
}
