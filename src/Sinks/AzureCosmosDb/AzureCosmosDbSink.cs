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
using System.IO;
using System.Linq;
using System.Net;
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

namespace Serilog.Sinks.AzureCosmosDB
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "<Pending>")]
    internal class AzureCosmosDBSink : IBatchedLogEventSink
    {
        private const string BulkStoredProcedureId = "BulkImport";
        private readonly CosmosClient _client;
        private readonly IFormatProvider _formatProvider;
        private readonly bool _storeTimestampInUtc;
        private readonly int? _timeToLive;
        private Database _database;
        private Container _container;
        private readonly string _partitionKey = "UtcDate";
        private readonly IPartitionKeyProvider _partitionKeyProvider;

        public AzureCosmosDBSink(
            AzureCosmosDbSinkOptions options)
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

            _client = new CosmosClientBuilder(options.EndpointUri.ToString(), options.AuthorizationKey)
              .WithCustomSerializer(new NewtonsoftJsonCosmosSerializer(serializerSettings))
              .WithConnectionModeGateway()
              .Build();
            
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

        private async Task CreateBulkImportStoredProcedureAsync(bool dropExistingProc = false)
        {
            var currentAssembly = typeof(AzureCosmosDBSink).GetTypeInfo().Assembly;

            SelfLog.WriteLine("Getting required resource.");
            var resourceName = currentAssembly.GetManifestResourceNames()
                                              .FirstOrDefault(w => w.EndsWith("bulkImport.js", StringComparison.InvariantCultureIgnoreCase));

            if (string.IsNullOrEmpty(resourceName))
            {
                SelfLog.WriteLine("Unable to find required resource.");

                return;
            }

            using (var resourceStream = currentAssembly.GetManifestResourceStream(resourceName))
            {
                if (resourceStream != null)
                {
                    using (var reader = new StreamReader(resourceStream))
                    {
                        var bulkImportSrc = await reader.ReadToEndAsync().ConfigureAwait(false);
                        try
                        {
                            var sprocExists = await CheckStoredProcedureExists(BulkStoredProcedureId);
                            if (sprocExists && dropExistingProc)
                            {
                                await _container.Scripts.DeleteStoredProcedureAsync(BulkStoredProcedureId);
                                sprocExists = false;
                            }

                            if (!sprocExists)
                            {
                                await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties()
                                {
                                    Id = BulkStoredProcedureId,
                                    Body = bulkImportSrc,
                                });
                            }
                        }
                        catch (Exception ex)
                        {
                            
                            SelfLog.WriteLine("Failed to update bulk stored procedure. {0}", ex);
                        }
                    }
                }
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
                    parameters: new dynamic[] { args });
                if (storedProcedureResponse.StatusCode != HttpStatusCode.OK)
                {
                    SelfLog.WriteLine("Unknown error writing to Cosmos. {0}", storedProcedureResponse.Diagnostics.ToString());
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



        #endregion

        #region Retry

        private class LogShipResult
        {
            public bool Success { get; set; }
            public Exception Exception { get; set; }
            public bool CanRetry { get; set; }
            public int RetryMilliseconds { get; set; }
        }
        #endregion

    }
}
