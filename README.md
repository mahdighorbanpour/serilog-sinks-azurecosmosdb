# Serilog.Sinks.AzCosmosDB
[![.NET](https://github.com/tghamm/serilog-sinks-azcosmosdb/actions/workflows/dotnet.yml/badge.svg)](https://github.com/tghamm/serilog-sinks-azcosmosdb/actions/workflows/dotnet.yml)

A Serilog sink that writes to Azure CosmosDB and supports PartitionKey and PeriodicBatching for better performance. This code is based on [serilog-sinks-azurecosmosdb](https://github.com/mahdighorbanpour/serilog-sinks-azurecosmosdb) and adapted to use modern patterns and practices.

## Getting started
You can start by installing the [NuGet package](https://www.nuget.org/packages/Serilog.Sinks.AzCosmosDB).



Configure logger by calling `WriteTo.AzCosmosDB(<client>, <options>)`

```C#
var builder =
    new CosmosClientBuilder(configuration["AppSettings:AzureCosmosUri"], configuration["AppSettings:AzureCosmosKey"])
        .WithConnectionModeGateway();
var client = builder.Build();

var logger = new LoggerConfiguration()
    .WriteTo.AzCosmosDB(client, new AzCosmosDbSinkOptions()
    {
        DatabaseName = "TestDb"
    })
    .CreateLogger();
```
## PartitionKey

Default partition key name is <b>/UtcDate</b> althought it can be overrided using parameter like below

```C#
Log.Logger = new LoggerConfiguration()
                .WriteTo.AzCosmosDB(client, new AzCosmosDbSinkOptions()
                    {
                        DatabaseName = "TestDb",
                        PartitionKey = "MyCustomKeyName"
                    })
                .CreateLogger();
```

## IPartitionKeyProvider
The DefaultPartitionkeyProvide will generate a utc date string with the format "dd.MM.yyyy". If you want to override it, you need to define a class and implement IPartitionKeyProvider interface and pass an instance of it in the arguments list.

## TTL (Time-to-live)

Azure CosmosDB is making easier to prune old data with support of Time To Live (TTL) so does Sink. AzCosmosDB Sink offers TTL at two levels.

### Enable TTL at collection level.

Sink supports TTL at collection level, if collection does not already exist.
 
To enable TTL at collection level, set **TimeToLive** parameter in code.

```C#
.WriteTo.AzCosmosDB(client, new new AzCosmosDbSinkOptions() { TimeToLive = TimeSpan.FromDays(7)})
```
If collection in CosmosDB doesn't exists, it will create one and set TTL on collection level causing all logs messages purge older than 7 days.


### Enable TTL at inidividual log message level.

Sink do support TTL at individual message level. This allows developer to retian log message of high importance longer than of lesser importance.

```C#
logger.Information("This message will expire and purge automatically after {@_ttl} seconds", 60);

logger.Information("Log message will be retained for 30 days {@_ttl}", 2592000); // 30*24*60*60

logger.Information("Messages of high importance will never expire {@_ttl}", -1); 
```

See [TTL behavior](https://docs.microsoft.com/en-us/azure/cosmos-db/time-to-live) in CosmosDB documentation for in depth explianation.

>Note: `{@_ttl}` is a reserved expression for TTL.

## Performance
Sink buffers log internally and flush to Azure CosmosDB in batches using `Serilog.Sinks.PeriodicBatching` and is configurable. However, it highly depends on type of Azure CosmosDB subscription you have. 
