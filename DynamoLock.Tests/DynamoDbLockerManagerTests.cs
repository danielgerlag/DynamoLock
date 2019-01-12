using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DynamoLock.Tests
{
    [Collection("DynamoDb collection")]
    public class DynamoDbLockerManagerTests
    {
        DynamoDbDockerSetup _dockerSetup;
        private IDistributedLockManager _subject;
        private TimeSpan _heartbeat = TimeSpan.FromSeconds(2);
        private long _leaseTime = 3;

        public DynamoDbLockerManagerTests(DynamoDbDockerSetup dockerSetup)
        {
            _dockerSetup = dockerSetup;
            var cfg = new AmazonDynamoDBConfig { ServiceURL = DynamoDbDockerSetup.ConnectionString };
            var provisioner = new LockTableProvisioner(DynamoDbDockerSetup.Credentials, cfg, "lock-tests", new NullLoggerFactory());
            _subject = new DynamoDbLockManager(DynamoDbDockerSetup.Credentials, cfg, "lock-tests", provisioner, new NullLoggerFactory(), _leaseTime, _heartbeat);
        }
        
        [Fact]
        public async void should_lock_resource()
        {
            var lockId = Guid.NewGuid().ToString();

            await _subject.Start();
            var first = await _subject.AcquireLock(lockId);
            var second = await _subject.AcquireLock(lockId);
            await _subject.Stop();

            Assert.True(first);
            Assert.False(second);
        }

        [Fact]
        public async void should_release_lock()
        {
            var lockId = Guid.NewGuid().ToString();

            await _subject.Start();
            var first = await _subject.AcquireLock(lockId);
            await _subject.ReleaseLock(lockId);
            var second = await _subject.AcquireLock(lockId);
            await _subject.Stop();

            Assert.True(first);
            Assert.True(second);
        }

        [Fact]
        public async void should_renew_lock_when_heartbeat_active()
        {
            var lockId = Guid.NewGuid().ToString();

            await _subject.Start();
            var first = await _subject.AcquireLock(lockId);
            await Task.Delay(TimeSpan.FromSeconds(_leaseTime + 2));
            var second = await _subject.AcquireLock(lockId);
            await _subject.Stop();

            Assert.True(first);
            Assert.False(second);
        }

        [Fact]
        public async void should_expire_lock_when_heartbeat_inactive()
        {
            var lockId = Guid.NewGuid().ToString();

            await _subject.Start();
            var first = await _subject.AcquireLock(lockId);
            await _subject.Stop();
            await Task.Delay(TimeSpan.FromSeconds(_leaseTime + 2));
            await _subject.Start();
            var second = await _subject.AcquireLock(lockId);
            await _subject.Stop();

            Assert.True(first);
            Assert.True(second);
        }
    }
}
