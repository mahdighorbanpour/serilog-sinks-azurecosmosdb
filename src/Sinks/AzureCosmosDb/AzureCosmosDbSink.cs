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
using Serilog.Sinks.Batch;
using Serilog.Sinks.Extensions;

namespace Serilog.Sinks.AzureCosmosDB
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "<Pending>")]
    internal class AzureCosmosDBSink : BatchProvider, ILogEventSink
    {
        private const string BulkStoredProcedureId = "BulkImport";
        private readonly CosmosClient _client;
        private readonly IFormatProvider _formatProvider;
        private readonly bool _storeTimestampInUtc;
        private readonly int? _timeToLive;
        private Database _database;
        private Container _container;
        private readonly SemaphoreSlim _semaphoreSlim;
        private readonly string _partiotionKey = "UtcDate";
        private readonly IPartitionKeyProvider _partitionKeyProvider;

        public AzureCosmosDBSink(
            Uri endpointUri,
            string authorizationKey,
            string databaseName,
            string collectionName,
            string partitionKey,
            IPartitionKeyProvider partitionKeyProvider,
            IFormatProvider formatProvider,
            bool storeTimestampInUtc,
            TimeSpan? timeToLive,
            int logBufferSize = 25_000,
            int batchSize = 100) : base(batchSize, logBufferSize)
        {
            _formatProvider   = formatProvider;
            _partiotionKey = partitionKey;
            _partitionKeyProvider = partitionKeyProvider ?? new DefaultPartitionKeyProvider(_formatProvider);

            if ((timeToLive != null) && (timeToLive.Value != TimeSpan.MaxValue))
                _timeToLive = (int) timeToLive.Value.TotalSeconds;

            _storeTimestampInUtc = storeTimestampInUtc;
            _semaphoreSlim       = new SemaphoreSlim(1, 1);

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
                     SelfLog.WriteLine("Serialization Error: " + args.ErrorContext.Error.Message);
                     args.ErrorContext.Handled = true;
                 }
             };

            _client = new CosmosClientBuilder(endpointUri.ToString(), authorizationKey)
              .WithCustomSerializer(new NewtonsoftJsonCosmosSerializer(serializerSettings))
              .WithConnectionModeGateway()
              .Build();
            
            CreateDatabaseAndContainerIfNotExistsAsync(databaseName, collectionName, partitionKey).Wait();
        }

        private async Task CreateDatabaseAndContainerIfNotExistsAsync(string databaseName, string containerName, string partitionKey)
        {
            SelfLog.WriteLine($"Opening database {databaseName}");
            _database = (await _client.CreateDatabaseIfNotExistsAsync(databaseName)).Database;
            _container = _database.GetContainer(containerName) ??
                (await _database.DefineContainer(containerName, $"/{partitionKey}")
                    // Define time to live (TTL) in seconds on container
                    .WithDefaultTimeToLive(-1)
                    .CreateAsync())
                    .Container;
            await CreateBulkImportStoredProcedureAsync().ConfigureAwait(false);
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
                            SelfLog.WriteLine(ex.Message);
                        }
                    }
                }
            }
        }

        private async Task<bool> CheckStoredProcedureExists(string id)
        {
            Scripts scripts = _container.Scripts;
            string queryText = "SELECT * FROM s where s.id = '@testId'";
            QueryDefinition queryDefinition = new QueryDefinition(queryText);
            queryDefinition.WithParameter("@testId", id);
            using (var iter = scripts.GetStoredProcedureQueryIterator<StoredProcedureResponse>(queryDefinition))
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

        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            try
            {
                if (logEventsBatch == null || logEventsBatch.Count == 0)
                    return true;

                var args = logEventsBatch.Select(x => x.Dictionary(_storeTimestampInUtc, _formatProvider));

                string _partitionKeyValue = _partitionKeyProvider.GeneratePartitionKey(logEventsBatch.FirstOrDefault());

                args = args.Select(
                   x =>
                   {
                       if (!x.Keys.Contains(_partiotionKey))
                           x.Add(_partiotionKey, _partitionKeyValue);
                       return x;
                   });
                
                if ((_timeToLive != null) && (_timeToLive > 0))
                    args = args.Select(
                        x =>
                        {
                            if (!x.Keys.Contains("ttl"))
                                x.Add("ttl", _timeToLive);
                            if (!x.Keys.Contains(_partiotionKey))
                                x.Add(_partiotionKey, _partitionKeyValue);
                            return x;
                        });
                await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
                try
                {
                    SelfLog.WriteLine($"Sending batch of {logEventsBatch.Count} messages to CosmosDB");
                    var storedProcedureResponse = await _container.Scripts.ExecuteStoredProcedureAsync<int>(
                    storedProcedureId: BulkStoredProcedureId, 
                    partitionKey: new PartitionKey(_partitionKeyValue),
                    parameters: new dynamic[] { args });

                    SelfLog.WriteLine(storedProcedureResponse.StatusCode.ToString());

                    return storedProcedureResponse.StatusCode == HttpStatusCode.OK;
                }
                catch (AggregateException e)
                {
                    SelfLog.WriteLine($"ERROR: {(e.InnerException ?? e).Message}");

                    var exception = e.InnerException as CosmosException;
                    if (exception != null)
                    {
                        var ei = (CosmosException)e.InnerException;
                        if (ei?.StatusCode != null)
                        {
                            exception = ei;
                        }
                    }

                    if (exception?.StatusCode == null)
                        return false;

                    switch ((int)exception.StatusCode)
                    {
                        case 429:
                            var delayTask = Task.Delay(TimeSpan.FromMilliseconds(exception.RetryAfter.Value.Milliseconds + 10));
                            await delayTask;

                            break;
                        default:
                            await CreateBulkImportStoredProcedureAsync(true).ConfigureAwait(false);

                            break;
                    }

                    return false;
                }
                finally
                {
                    _semaphoreSlim.Release();
                }
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);
                    return false;
            }
        }

        #endregion

        #region ILogEventSink Support

        public void Emit(LogEvent logEvent)
        {
            if (logEvent != null) {
                PushEvent(logEvent);
            }
        }

        #endregion
    }
}
