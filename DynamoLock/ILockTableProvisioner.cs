using System.Threading.Tasks;

namespace DynamoLock
{
    public interface ILockTableProvisioner
    {
        Task Provision();
    }
}