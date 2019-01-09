using System;
using System.Threading;
using System.Threading.Tasks;

namespace DynamoLock
{
    public interface IDistributedLockManager
    {
        Task<bool> AcquireLock(string Id);

        Task ReleaseLock(string Id);

        Task Start();

        Task Stop();
    }

}
