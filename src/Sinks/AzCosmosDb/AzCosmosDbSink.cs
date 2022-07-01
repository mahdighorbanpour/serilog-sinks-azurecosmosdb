// Copyright 2016 Serilog Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.Extensions;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.AzCosmosDB
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "<Pending>")]
    internal class AzCosmosDbSink : IBatchedLogEventSink
    {
        private const string BulkStoredProcedureId = "BulkImport";
        private const string BulkStoredProcedureVersionId = "BulkImportVersion";
        private const string ExpectedProcedureVersion = "2.0.0";
        private readonly CosmosClient _client;
        private readonly IFormatProvider _formatProvider;
        private readonly bool _storeTimestampInUtc;
        private readonly int? _timeToLive;
        private Database _database;
        private Container _container;
        private readonly string _partitionKey = "UtcDate";
        private readonly IPartitionKeyProvider _partitionKeyProvider;

        public AzCosmosDbSink(CosmosClient client, AzCosmosDbSinkOptions options)
        {
            _formatProvider = options.FormatProvider;
            _partitionKey = options.PartitionKey;
            _partitionKeyProvider = options.PartitionKeyProvider ?? new DefaultPartitionKeyProvider(_formatProvider);

            if ((options.TimeToLive != null) && (options.TimeToLive.Value != TimeSpan.MaxValue))
                _timeToLive = (int)options.TimeToLive.Value.TotalSeconds;

            _storeTimestampInUtc = options.StoreTimestampInUTC;

            var serializerSettings = new JsonSerializerSettings
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                ContractResolver = new DefaultContractResolver(),
            };
            serializerSettings.Error += (object sender, Newtonsoft.Json.Serialization.ErrorEventArgs args) =>
            {
                // only log an error once
                if (args.CurrentObject == args.ErrorContext.OriginalObject)
                {
                    SelfLog.WriteLine("Serialization Error: {0}" + args.ErrorContext.Error);
                    args.ErrorContext.Handled = true;
                }
            };
            client.ClientOptions.Serializer = new NewtonsoftJsonCosmosSerializer(serializerSettings);
            _client = client;
            CreateDatabaseAndContainerIfNotExistsAsync(options.DatabaseName, options.CollectionName, options.PartitionKey).Wait();
        }

        public AzCosmosDbSink(AzCosmosDbSinkOptions options)
        {
            _formatProvider   = options.FormatProvider;
            _partitionKey = options.PartitionKey;
            _partitionKeyProvider = options.PartitionKeyProvider ?? new DefaultPartitionKeyProvider(_formatProvider);

            if ((options.TimeToLive != null) && (options.TimeToLive.Value != TimeSpan.MaxValue))
                _timeToLive = (int)options.TimeToLive.Value.TotalSeconds;

            _storeTimestampInUtc = options.StoreTimestampInUTC;

            var serializerSettings = new JsonSerializerSettings
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                ContractResolver = new DefaultContractResolver(),                
            };
            serializerSettings.Error += (object sender, Newtonsoft.Json.Serialization.ErrorEventArgs args) =>
             {
                // only log an error once
                if (args.CurrentObject == args.ErrorContext.OriginalObject)
                {
                    SelfLog.WriteLine("Serialization Error: {0}" + args.ErrorContext.Error);
                    args.ErrorContext.Handled = true;
                }
             };

            var builder = new CosmosClientBuilder(options.EndpointUri.ToString(), options.AuthorizationKey)
              .WithCustomSerializer(new NewtonsoftJsonCosmosSerializer(serializerSettings))
              .WithConnectionModeGateway();

            if (options.DisableSSL)
                builder.WithHttpClientFactory(() =>
                {
                    HttpMessageHandler httpMessageHandler = new HttpClientHandler()
                    {
                        ServerCertificateCustomValidationCallback = (w, x, y, z) => true,

                    };
                    return new HttpClient(httpMessageHandler);
                });

            _client = builder.Build();

            CreateDatabaseAndContainerIfNotExistsAsync(options.DatabaseName, options.CollectionName, options.PartitionKey).Wait();
        }

        private async Task CreateDatabaseAndContainerIfNotExistsAsync(string databaseName, string containerName, string partitionKey)
        {
            SelfLog.WriteLine("Opening database {0}", databaseName);
            try
            {
                _database = (await _client.CreateDatabaseIfNotExistsAsync(databaseName)).Database;
                _container =
                    (await _database.CreateContainerIfNotExistsAsync(new ContainerProperties(containerName,
                        $"/{partitionKey}")
                    {
                        DefaultTimeToLive = -1
                    })).Container;

                await CreateBulkImportStoredProcedureAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine("Error initializing Cosmos logger. {0}", ex);
            }
            
        }

        private Version GetCurrentAssemblyVersion()
        {
            var currentAssembly = typeof(AzCosmosDbSink).GetTypeInfo().Assembly;
            return currentAssembly.GetName().Version;
        }


        private async Task<string> GetScriptContents(string scriptName)
        {
            var currentAssembly = typeof(AzCosmosDbSink).GetTypeInfo().Assembly;

            SelfLog.WriteLine("Getting required resource.");
            var resourceName = currentAssembly.GetManifestResourceNames()
                .FirstOrDefault(w => w.EndsWith(scriptName, StringComparison.InvariantCultureIgnoreCase));

            if (string.IsNullOrEmpty(resourceName))
            {
                SelfLog.WriteLine("Unable to find required resource.");

                return null;
            }

            using (var resourceStream = currentAssembly.GetManifestResourceStream(resourceName))
            {
                if (resourceStream != null)
                {
                    using (var reader = new StreamReader(resourceStream))
                    {
                        var bulkImportSrc = await reader.ReadToEndAsync().ConfigureAwait(false);
                        return bulkImportSrc;
                    }
                }
            }

            return null;
        }

        private async Task CreateBulkImportStoredProcedureAsync()
        {

            try
            {
                var versionSprocExists = await CheckStoredProcedureExists(BulkStoredProcedureVersionId);
                var versionValid = false;
                var expectedVersion = GetCurrentAssemblyVersion();
                if (versionSprocExists)
                {
                    //check version
                    var version = new Version((await TryGetCosmosBulkImportVersion()).Version);
                    if (version == expectedVersion)
                    {
                        versionValid = true;
                    }
                }
                else
                {
                    await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties()
                    {
                        Id = BulkStoredProcedureVersionId,
                        Body = (await GetScriptContents("bImportVersionCheck.js")).Replace("#ASSEMBLYVERSION#", expectedVersion.ToString()),
                    });
                }

                var sprocExists = await CheckStoredProcedureExists(BulkStoredProcedureId);
                if (sprocExists && !versionValid)
                {
                    await _container.Scripts.DeleteStoredProcedureAsync(BulkStoredProcedureId);
                    await _container.Scripts.DeleteStoredProcedureAsync(BulkStoredProcedureVersionId);
                    sprocExists = false;
                }

                if (!sprocExists)
                {
                    await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties()
                    {
                        Id = BulkStoredProcedureId,
                        Body = await GetScriptContents("bulkImport.js"),
                    });
                    await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties()
                    {
                        Id = BulkStoredProcedureVersionId,
                        Body = (await GetScriptContents("bImportVersionCheck.js")).Replace("#ASSEMBLYVERSION#", expectedVersion.ToString()),
                    });
                }
            }
            catch (Exception ex)
            {

                SelfLog.WriteLine("Failed to update bulk stored procedure. {0}", ex);
            }
        }



        private async Task<bool> CheckStoredProcedureExists(string id)
        {
            
            Scripts scripts = _container.Scripts;
            string queryText = "SELECT * FROM s where s.id = @testId";
            QueryDefinition queryDefinition = new QueryDefinition(queryText);
            queryDefinition.WithParameter("@testId", id);
            using (var iter = scripts.GetStoredProcedureQueryIterator<StoredProcedureProperties>(queryDefinition))
            {
                while (iter.HasMoreResults)
                {
                    // Stream iterator returns a response with status for errors
                    var response = await iter.ReadNextAsync();
                    
                        // Handle failure scenario. 
                        if (response.FirstOrDefault() != null)
                        {
                            return true;
                        }
                }
            }
            return false;
        }

        #region Parallel Log Processing Support

        public async Task EmitBatchAsync(IEnumerable<LogEvent> batch)
        {
            try
            {
                if (batch == null) return;
                var logEvents = batch.ToList();
                if (!logEvents.Any())
                    return;

                var args = logEvents.Select(x => x.Dictionary(_storeTimestampInUtc, _formatProvider));

                string partitionKeyValue = _partitionKeyProvider.GeneratePartitionKey(logEvents.FirstOrDefault());

                args = args.Select(
                   x =>
                   {
                       if (!x.Keys.Contains(_partitionKey))
                           x.Add(_partitionKey, partitionKeyValue);
                       return x;
                   });

                if ((_timeToLive != null) && (_timeToLive > 0))
                    args = args.Select(
                        x =>
                        {
                            if (!x.Keys.Contains("ttl"))
                                x.Add("ttl", _timeToLive);
                            if (!x.Keys.Contains(_partitionKey))
                                x.Add(_partitionKey, partitionKeyValue);
                            return x;
                        });

                var firstAttempt = await TryShipLogsToCosmos(partitionKeyValue, args);

                if (!firstAttempt.Success && firstAttempt.CanRetry)
                {
                    await Task.Delay(firstAttempt.RetryMilliseconds);
                    var secondAttempt = await TryShipLogsToCosmos(partitionKeyValue, args);
                    if (!secondAttempt.Success)
                    {
                        SelfLog.WriteLine("Error writing to cosmos after retry. {0}", secondAttempt.Exception);
                    }
                }
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine("Uncaught exception logging to Cosmos. {0}", ex);

            }
        }

        public Task OnEmptyBatchAsync()
        {
            return Task.CompletedTask;
        }

        private async Task<LogShipResult> TryShipLogsToCosmos(string partitionKeyValue, IEnumerable<IDictionary<string, object>> args)
        {
            try
            {
                var storedProcedureResponse = await _container.Scripts.ExecuteStoredProcedureAsync<int>(
                    storedProcedureId: BulkStoredProcedureId,
                    partitionKey: new PartitionKey(partitionKeyValue),
                    parameters: new dynamic[] { args },
                    new StoredProcedureRequestOptions()
                    {
                        EnableScriptLogging = true
                    });
                if (storedProcedureResponse.StatusCode != HttpStatusCode.OK)
                {
                    SelfLog.WriteLine("Unknown error writing to Cosmos. {0}", storedProcedureResponse.Diagnostics.ToString());
                }

                if (!string.IsNullOrWhiteSpace(storedProcedureResponse.ScriptLog))
                {
                    SelfLog.WriteLine("Error writing at least one log to Cosmos. {0}", storedProcedureResponse.ScriptLog);
                }
                return new LogShipResult()
                {
                    Success = storedProcedureResponse.StatusCode == HttpStatusCode.OK,
                };
            }
            catch (CosmosException ex)
            {
                int retryPeriod = 0;
                var canRetry = false;
                switch ((int)ex.StatusCode)
                {
                    case 429:
                        if (ex.RetryAfter != null)
                        {
                            retryPeriod = Convert.ToInt32(Math.Round(ex.RetryAfter.Value.TotalMilliseconds)) + 10;
                            canRetry = true;
                        }

                        break;
                }

                return new LogShipResult()
                {
                    Success = false,
                    CanRetry = canRetry,
                    RetryMilliseconds = retryPeriod,
                    Exception = ex
                };
            }
        }

        private async Task<BulkInsertVersionResult> TryGetCosmosBulkImportVersion()
        {
            var storedProcedureResponse = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
                storedProcedureId: BulkStoredProcedureVersionId,
                partitionKey: new PartitionKey("test"),
                parameters: new dynamic[] { },
                new StoredProcedureRequestOptions()
                {
                    EnableScriptLogging = true
                });
            if (storedProcedureResponse.StatusCode != HttpStatusCode.OK)
            {
                SelfLog.WriteLine("Unknown error writing to Cosmos. {0}", storedProcedureResponse.Diagnostics.ToString());
            }

            if (!string.IsNullOrWhiteSpace(storedProcedureResponse.ScriptLog))
            {
                SelfLog.WriteLine("Error writing at least one log to Cosmos. {0}", storedProcedureResponse.ScriptLog);
            }
            return new BulkInsertVersionResult()
            {
                Version = storedProcedureResponse.Resource
            };
        }



        #endregion

        #region Retry

        private class LogShipResult
        {
            public bool Success { get; set; }
            public Exception Exception { get; set; }
            public bool CanRetry { get; set; }
            public int RetryMilliseconds { get; set; }
        }

        private class BulkInsertVersionResult
        {
            public string Version { get; set; }
        }
        #endregion

    }
}
