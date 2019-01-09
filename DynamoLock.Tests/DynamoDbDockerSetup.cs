using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using Docker.Testify;
using Xunit;

namespace DynamoLock.Tests
{
    public class DynamoDbDockerSetup : DockerSetup
    {
        public static string ConnectionString { get; set; }

        public static AWSCredentials Credentials => new EnvironmentVariablesAWSCredentials();

        public override string ImageName => @"amazon/dynamodb-local";
        public override int InternalPort => 8000;

        public override void PublishConnectionInfo()
        {
            ConnectionString = $"http://localhost:{ExternalPort}";
        }

        public override bool TestReady()
        {
            try
            {
                AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig
                {
                    ServiceURL = $"http://localhost:{ExternalPort}"
                };
                AmazonDynamoDBClient client = new AmazonDynamoDBClient(clientConfig);
                var resp = client.ListTablesAsync().Result;

                return resp.HttpStatusCode == HttpStatusCode.OK;
            }
            catch
            {
                return false;
            }

        }
    }

    [CollectionDefinition("DynamoDb collection")]
    public class DynamoDbCollection : ICollectionFixture<DynamoDbDockerSetup>
    {
    }
}
