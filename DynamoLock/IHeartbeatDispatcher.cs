using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace DynamoLock
{
    public interface IHeartbeatDispatcher
    {
        void Start(string nodeId, TimeSpan heartbeat, long leaseTime);
        void Stop();
    }
}
