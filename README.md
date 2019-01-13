
# DynamoLock

DynamoLock is a client library for .Net Standard that implements a distributed lock manager on top of Amazon DynamoDB.


## Installing

Using Nuget package console
```
PM> Install-Package DynamoLock
```
Using .NET CLI
```
dotnet add package DynamoLock
```


## How it works

When you successfully acquire a lock, an entry is written to a table in DynamoDB that indicates the owner of the lock and the expiry time.
The default lease time is 30 seconds, each node will also send a heartbeat every 10 seconds that will renew any active leases for 30 seconds from the time of the heartbeat.
Once the expiry time has elapsed or the owning node releases the lock, it becomes available again.

## Usage

1. An extension method to the IoC abstraction of `IServiceCollection` is provided that will add an implementation of `IDistributedLockManager` to your IoC container.

    ```c#
    using DynamoLock;
    ...

    services.AddDynamoLockManager(new EnvironmentVariablesAWSCredentials(), new AmazonDynamoDBConfig() { RegionEndpoint = RegionEndpoint.USWest2 }, "lock_table");

    ```

2. Then inject `IDistributedLockManager` into your classes via your IoC container of choice.
3. When your application starts up, you will also need to call the `.Start` method in order to provision the table and start the background heartbeat service.
    ```c#
    IDistributedLockManager lockerManager;
    ...
    lockManager.Start();
    ```
    This will automatically provision the table in DynamoDB if it does not exist and will enable sending a heartbeat every 10 seconds for any locks that have been acquired with the local manager, and renew their leases for a further 30 seconds, until `ReleaseLock` is called for a given resource.
    The table will be created with through put units of 1 by default, you can change these values on the AWS console after the fact.

4. Use the `AcquireLock` and `ReleaseLock` methods to manage your distributed locks.

    ```c#
    IDistributedLockManager lockerManager;
    ...
    var success = await lockManager.AcquireLock("my-lock-id");
    ...
    await lockManager.ReleaseLock("my-lock-id");
    ```

    `AcquireLock` will return `false` if the lock is already in use and `true` if it successfully acquired the lock.
    It will start with an initial lease of 30 seconds, and a background thread will renew all locally controlled leases every 10 seconds, until `ReleaseLock` is called or the application ends and the leases expire naturally.

### Notes

It is recomended that you keep the clocks of all particapting nodes in sync using an NTP implementation

* https://en.wikipedia.org/wiki/Network_Time_Protocol
* https://aws.amazon.com/blogs/aws/keeping-time-with-amazon-time-sync-service/

## Authors
 * **Daniel Gerlag** - daniel@gerlag.ca

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE.md) file for details
