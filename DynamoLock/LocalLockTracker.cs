using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace DynamoLock
{
    public class LocalLockTracker : ILocalLockTracker
    {
        private readonly List<string> _localLocks = new List<string>();
        private readonly AutoResetEvent _mutex = new AutoResetEvent(true);

        public void Add(string id)
        {
            _mutex.WaitOne();

            try
            {
                _localLocks.Add(id);
            }
            finally
            {
                _mutex.Set();
            }
        }

        public void Remove(string id)
        {
            _mutex.WaitOne();

            try
            {
                _localLocks.Remove(id);
            }
            finally
            {
                _mutex.Set();
            }
        }

        public void Clear()
        {
            _mutex.WaitOne();
            try
            {
                _localLocks.Clear();
            }
            finally
            {
                _mutex.Set();
            }
        }

        public ICollection<string> GetSnapshot()
        {
            _mutex.WaitOne();
            try
            {
                return _localLocks.ToArray();
            }
            finally
            {
                _mutex.Set();
            }
        }
    }
}
