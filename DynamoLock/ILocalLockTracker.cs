using System.Collections.Generic;

namespace DynamoLock
{
    public interface ILocalLockTracker
    {
        void Add(string id);
        void Remove(string id);
        void Clear();
        ICollection<string> GetSnapshot();
    }
}