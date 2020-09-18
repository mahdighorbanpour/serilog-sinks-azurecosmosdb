# Serilog.Sinks.AzureCosmosDB
A Serilog sink that writes to Azure CosmosDB and supports PartitionKey for better performance. This code is based on [serilog-sinks-azuredocumentdb](https://github.com/serilog/serilog-sinks-azuredocumentdb) and adapted to use the latest Microsof.Azure.Cosmos SDK version 3.12.0 and uses a custom JsonSerializationSettings which helps to keep the sink alive when serialization fails sometimes specially for Exceptions!

## Getting started
Nuget package is on the way to be prepared! please be patient.



Configure logger by calling `WriteTo.AzureCosmosDB(<uri>, <key>)`

```C#
var logger = new LoggerConfiguration()
    .WriteTo.AzureDocumentDB(<uri>, <secure-key>)
    .CreateLogger();
```
## PartitionKey

Default partition key name is <b>/UtcDate</b> althought it can be overrided using parameter like below

```C#
Log.Logger = new LoggerConfiguration()
                .WriteTo.AzureCosmosDB(
                    endpointUri: <uri>,
                    authorizationKey: <secure-key>,
                    partitionKey: "MyCustomKeyName"
                )
                .CreateLogger();
```

## IPartitionKeyProvider
The DefaultPartitionkeyProvide will generate a utc date string with the format "dddd.MM.yyyy". If you want to override it, you need to define a class and implement IPartitionKeyProvider interface and pass an instance of it in the arguments list.

## TTL (Time-to-live)

Azure DocumentDB is making easier to prune old data with support of Time To Live (TTL) so does Sink. AzureDocumentDB Sink offers TTL at two levels.

### Enable TTL at collection level.

Sink supports TTL at collection level, if collection does not already exist.
 
To enable TTL at collection level, set **timeToLive** parameter in code.

```C#
.WriteTo.AzureDocumentDB(<uri>, <secure-key>, timeToLive: TimeSpan.FromDays(7))
```
If collection in DocumentDB doesn't exists, it will create one and set TTL on collection level causing all logs messages purge older than 7 days.


### Enable TTL at inidividual log message level.

Sink do support TTL at individual message level. This allows developer to retian log message of high importance longer than of lesser importance.

```C#
logger.Information("This message will expire and purge automatically after {@_ttl} seconds", 60);

logger.Information("Log message will be retained for 30 days {@_ttl}", 2592000); // 30*24*60*60

logger.Information("Messages of high importance will never expire {@_ttl}", -1); 
```

See [TTL behavior](https://azure.microsoft.com/en-us/documentation/articles/documentdb-time-to-live/) in DocumentDB documentation for in depth explianation.

>Note: `{@_ttl}` is a reserved expression for TTL.



## XML <appSettings> configuration

To use the AzureDocumentDB sink with the [Serilog.Settings.AppSettings](https://www.nuget.org/packages/Serilog.Settings.AppSettings) package, first install that package if you haven't already done so:

```PowerShell
Install-Package Serilog.Settings.AppSettings
```
In your code, call `ReadFrom.AppSettings()`

```C#
var logger = new LoggerConfiguration()
    .ReadFrom.AppSettings()
    .CreateLogger();
```
In your application's App.config or Web.config file, specify the DocumentDB sink assembly and required **endpointUrl** and **authorizationKey** parameters under the `<appSettings>`

```XML
<appSettings>
  <add key="serilog:using:AzureDocumentDB" value="Serilog.Sinks.AzureDocumentDB" />
  <add key="serilog:write-to:AzureDocumentDB.endpointUrl" value="https://****.documents.azure.com:443" />
  <add key="serilog:write-to:AzureDocumentDB.authorizationKey" value="****" />
    
  <!-- Liefspan of log messages in DocumentDB in seconds, leave empty to disable expiration. -->
  <add key="serilog:write-to:AzureDocumentDB.timeToLive" value="60" />
</appSettings>
```

## Performance
Sink buffers log internally and flush to Azure DocumentDB in batches using dedicated thread. However, it highly depends on type of Azure DocumentDB subscription you have. 
