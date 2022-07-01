// Copyright 2020 Serilog Contributors
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
using Microsoft.Azure.Cosmos;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Sinks.AzCosmosDB;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog
{
    /// <summary>
    ///     Adds the WriteTo.AzCosmosDB() extension method to <see cref="LoggerConfiguration" />.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "<Pending>")]
    public static class LoggerConfigurationAzureCosmosDBExtensions
    {

        /// <summary>
        ///     Adds a sink that writes log events to a Azure CosmosDB table in the provided endpoint.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="client">The cosmos client to use for operations</param>
        /// <param name="databaseName">The name of the database to use; will create if it doesn't exist.</param>
        /// <param name="collectionName">The name of the collection to use inside the database; will created if it doesn't exist.</param>
        /// <param name="partitionKey">The name of partition key for the collection.</param>
        /// <param name="partitionKeyProvider">A value provider for partition key. If none provided, the default will be used.</param>
        /// <param name="restrictedToMinimumLevel">The minimum log event level required in order to write an event to the sink.</param>
        /// <param name="formatProvider">Supplies culture-specific formatting information, or null.</param>
        /// <param name="storeTimestampInUtc">Store Timestamp in UTC</param>
        /// <param name="timeToLive">The lifespan of documents in seconds. Set null to disable document expiration. </param>
        /// <param name="logBufferSize">Maximum number of log entries this sink can hold before stop accepting log messages. Supported size is between 5000 and 25000</param>
        /// <param name="batchSize">Number of log messages to be sent as batch. Supported range is between 1 and 1000</param>
        /// <param name="levelSwitch">
        /// A switch allowing the pass-through minimum level to be changed at runtime.
        /// </param>
        /// <exception cref="ArgumentNullException">A required parameter is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">A required parameter value is out of acceptable range.</exception>
        public static LoggerConfiguration AzCosmosDB(
                this LoggerSinkConfiguration loggerConfiguration,
                CosmosClient client,
                string databaseName = "Diagnostics",
                string collectionName = "Logs",
                string partitionKey = "UtcDate",
                IPartitionKeyProvider partitionKeyProvider = null,
                LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
                IFormatProvider formatProvider = null,
                bool storeTimestampInUtc = true,
                //Protocol connectionProtocol = Protocol.Https,
                TimeSpan? timeToLive = null,
                int logBufferSize = 25_000,
                int batchSize = 100,
                LoggingLevelSwitch levelSwitch = null
            )
        {
            if (loggerConfiguration == null)
            {
                throw new ArgumentNullException(nameof(loggerConfiguration));
            }

            if (client == null)
            {
                throw new ArgumentNullException(nameof(client));
            }

            if ((timeToLive != null) && (timeToLive.Value > TimeSpan.FromDays(24_855)))
            {
                throw new ArgumentOutOfRangeException(nameof(timeToLive));
            }

            var sinkOptions = new AzCosmosDbSinkOptions()
            {
                PartitionKey = partitionKey,
                FormatProvider = formatProvider,
                BatchPostingLimit = batchSize,
                CollectionName = collectionName,
                DatabaseName = databaseName,
                PartitionKeyProvider = partitionKeyProvider,
                Period = TimeSpan.FromSeconds(2),
                QueueSizeLimit = logBufferSize,
                StoreTimestampInUTC = storeTimestampInUtc,
                TimeToLive = timeToLive
            };

            var batchingOptions = new PeriodicBatchingSinkOptions
            {
                BatchSizeLimit = sinkOptions.BatchPostingLimit,
                Period = sinkOptions.Period,
                EagerlyEmitFirstEvent = true,
                QueueLimit = sinkOptions.QueueSizeLimit
            };

            var cosmosSink = new AzCosmosDbSink(client, sinkOptions);
            var batchingSink = new PeriodicBatchingSink(cosmosSink, batchingOptions);
            return loggerConfiguration.Sink(batchingSink,
                restrictedToMinimumLevel,
                levelSwitch);
        }
        /// <summary>
        ///     Adds a sink that writes log events to a Azure CosmosDB table in the provided endpoint.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="client">The cosmos client to use for operations</param>
        /// <param name="options">The cosmos configuration options to use for operations</param>
        /// <param name="restrictedToMinimumLevel">The minimum log event level required in order to write an event to the sink.</param>
        /// <param name="levelSwitch">
        /// A switch allowing the pass-through minimum level to be changed at runtime.
        /// </param>
        /// <exception cref="ArgumentNullException">A required parameter is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">A required parameter value is out of acceptable range.</exception>
        public static LoggerConfiguration AzCosmosDB(
                this LoggerSinkConfiguration loggerConfiguration,
                CosmosClient client,
                AzCosmosDbSinkOptions options,
                LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
                LoggingLevelSwitch levelSwitch = null)
        {
            if (loggerConfiguration == null)
            {
                throw new ArgumentNullException(nameof(loggerConfiguration));
            }

            if (client == null)
            {
                throw new ArgumentNullException(nameof(client));
            }

            if ((options.TimeToLive != null) && (options.TimeToLive.Value.TotalSeconds > TimeSpan.FromDays(24_855).TotalSeconds))
            {
                throw new ArgumentOutOfRangeException(nameof(options.TimeToLive));
            }

            var batchingOptions = new PeriodicBatchingSinkOptions
            {
                BatchSizeLimit = options.BatchPostingLimit,
                Period = options.Period,
                EagerlyEmitFirstEvent = true,
                QueueLimit = options.QueueSizeLimit
            };

            var cosmosSink = new AzCosmosDbSink(client, options);
            var batchingSink = new PeriodicBatchingSink(cosmosSink, batchingOptions);
            return loggerConfiguration.Sink(batchingSink,
                restrictedToMinimumLevel,
                levelSwitch);
        }

        /// <summary>
        ///     Adds a sink that writes log events to a Azure CosmosDB table in the provided endpoint.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="endpointUri">The endpoint URI of the document db.</param>
        /// <param name="authorizationKey">The authorization key of the db.</param>
        /// <param name="databaseName">The name of the database to use; will create if it doesn't exist.</param>
        /// <param name="collectionName">The name of the collection to use inside the database; will created if it doesn't exist.</param>
        /// <param name="partitionKey">The name of partition key for the collection.</param>
        /// <param name="partitionKeyProvider">A value provider for partition key. If none provided, the default will be used.</param>
        /// <param name="restrictedToMinimumLevel">The minimum log event level required in order to write an event to the sink.</param>
        /// <param name="formatProvider">Supplies culture-specific formatting information, or null.</param>
        /// <param name="storeTimestampInUtc">Store Timestamp in UTC</param>
        /// <param name="timeToLive">
        ///     The lifespan of documents (roughly 24855 days maximum). Set null to disable document
        ///     expiration.
        /// </param>
        /// <param name="logBufferSize">Maximum number of log entries this sink can hold before stop accepting log messages. Supported size is between 5000 and 25000</param>
        /// <param name="batchSize">Number of log messages to be sent as batch. Supported range is between 1 and 1000</param>
        /// <param name="levelSwitch">
        /// A switch allowing the pass-through minimum level to be changed at runtime.
        /// </param>
        /// <param name="disableSSL">Disables ssl on cosmos client. Should be used for testing purposes only</param>
        /// <exception cref="ArgumentNullException">A required parameter is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">A required parameter value is out of acceptable range.</exception>
        public static LoggerConfiguration AzCosmosDB(
            this LoggerSinkConfiguration loggerConfiguration,
            Uri endpointUri,
            string authorizationKey,
            string databaseName = "Diagnostics",
            string collectionName = "Logs",
            string partitionKey = "UtcDate",
            IPartitionKeyProvider partitionKeyProvider = null,
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            IFormatProvider formatProvider = null,
            bool storeTimestampInUtc = true,
            //Protocol connectionProtocol = Protocol.Https,
            TimeSpan? timeToLive = null,
            int logBufferSize = 25_000,
            int batchSize = 100,
            LoggingLevelSwitch levelSwitch = null,
            bool disableSSL = false)
        {
            if (loggerConfiguration == null)
            {
                throw new ArgumentNullException(nameof(loggerConfiguration));
            }

            if (endpointUri == null)
            {
                throw new ArgumentNullException(nameof(endpointUri));
            }

            if (authorizationKey == null)
            {
                throw new ArgumentNullException(nameof(authorizationKey));
            }

            if ((timeToLive != null) && (timeToLive.Value > TimeSpan.FromDays(24_855)))
            {
                throw new ArgumentOutOfRangeException(nameof(timeToLive));
            }

            var sinkOptions = new AzCosmosDbSinkOptions()
            {
                DisableSSL = disableSSL,
                EndpointUri = endpointUri,
                PartitionKey = partitionKey,
                FormatProvider = formatProvider,
                AuthorizationKey = authorizationKey,
                BatchPostingLimit = batchSize,
                CollectionName = collectionName,
                DatabaseName = databaseName,
                PartitionKeyProvider = partitionKeyProvider,
                Period = TimeSpan.FromSeconds(2),
                QueueSizeLimit = logBufferSize,
                StoreTimestampInUTC = storeTimestampInUtc,
                TimeToLive = timeToLive
            };

            var batchingOptions = new PeriodicBatchingSinkOptions
            {
                BatchSizeLimit = sinkOptions.BatchPostingLimit,
                Period = sinkOptions.Period,
                EagerlyEmitFirstEvent = true,
                QueueLimit = sinkOptions.QueueSizeLimit
            };

            var cosmosSink = new AzCosmosDbSink(sinkOptions);
            var batchingSink = new PeriodicBatchingSink(cosmosSink, batchingOptions);
            return loggerConfiguration.Sink(batchingSink,
                restrictedToMinimumLevel,
                levelSwitch);
        }


        /// <summary>
        ///     Adds a sink that writes log events to a Azure CosmosDB table in the provided endpoint.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="endpointUrl">The endpoint url of the document db.</param>
        /// <param name="authorizationKey">The authorization key of the db.</param>
        /// <param name="databaseName">The name of the database to use; will create if it doesn't exist.</param>
        /// <param name="collectionName">The name of the collection to use inside the database; will created if it doesn't exist.</param>
        /// <param name="partitionKey">The name of partition key for the collection.</param>
        /// <param name="partitionKeyProvider">A value provider for partition key. If none provided, the default will be used.</param>
        /// <param name="restrictedToMinimumLevel">The minimum log event level required in order to write an event to the sink.</param>
        /// <param name="formatProvider">Supplies culture-specific formatting information, or null.</param>
        /// <param name="storeTimestampInUtc">Store Timestamp in UTC</param>
        /// <param name="timeToLive">The lifespan of documents in seconds. Set null to disable document expiration. </param>
        /// <param name="logBufferSize">Maximum number of log entries this sink can hold before stop accepting log messages. Supported size is between 5000 and 25000</param>
        /// <param name="batchSize">Number of log messages to be sent as batch. Supported range is between 1 and 1000</param>
        /// <param name="levelSwitch">
        /// A switch allowing the pass-through minimum level to be changed at runtime.
        /// </param>
        /// <param name="disableSSL">Disables ssl on cosmos client. Should be used for testing purposes only</param>
        /// <exception cref="ArgumentNullException">A required parameter is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">A required parameter value is out of acceptable range.</exception>
        public static LoggerConfiguration AzCosmosDB(
            this LoggerSinkConfiguration loggerConfiguration,
            Uri endpointUrl,
            string authorizationKey,
            string databaseName = "Diagnostics",
            string collectionName = "Logs",
            string partitionKey = "UtcDate",
            IPartitionKeyProvider partitionKeyProvider = null,
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            IFormatProvider formatProvider = null,
            bool storeTimestampInUtc = false,
            int? timeToLive = null,
            int logBufferSize = 25_000,
            int batchSize = 100,
            LoggingLevelSwitch levelSwitch = null,
            bool disableSSL = false)
        {
            if (loggerConfiguration == null)
            {
                throw new ArgumentNullException(nameof(loggerConfiguration));
            }

            if (string.IsNullOrWhiteSpace(endpointUrl?.ToString()))
            {
                throw new ArgumentNullException(nameof(endpointUrl));
            }

            if (authorizationKey == null)
            {
                throw new ArgumentNullException(nameof(authorizationKey));
            }

            if ((timeToLive != null) && (timeToLive.Value > TimeSpan.FromDays(24_855).TotalSeconds))
            {
                throw new ArgumentOutOfRangeException(nameof(timeToLive));
            }

            TimeSpan? timeSpan = null;
            if (timeToLive != null)
            {
                timeSpan = TimeSpan.FromSeconds(Math.Max(-1, timeToLive.Value));
            }

            var sinkOptions = new AzCosmosDbSinkOptions()
            {
                DisableSSL = disableSSL,
                EndpointUri = endpointUrl,
                PartitionKey = partitionKey,
                FormatProvider = formatProvider,
                AuthorizationKey = authorizationKey,
                BatchPostingLimit = batchSize,
                CollectionName = collectionName,
                DatabaseName = databaseName,
                PartitionKeyProvider = partitionKeyProvider,
                Period = TimeSpan.FromSeconds(2),
                QueueSizeLimit = logBufferSize,
                StoreTimestampInUTC = storeTimestampInUtc,
                TimeToLive = timeSpan
            };

            var batchingOptions = new PeriodicBatchingSinkOptions
            {
                BatchSizeLimit = sinkOptions.BatchPostingLimit,
                Period = sinkOptions.Period,
                EagerlyEmitFirstEvent = true,
                QueueLimit = sinkOptions.QueueSizeLimit
            };

            var cosmosSink = new AzCosmosDbSink(sinkOptions);
            var batchingSink = new PeriodicBatchingSink(cosmosSink, batchingOptions);
            return loggerConfiguration.Sink(batchingSink,
                restrictedToMinimumLevel,
                levelSwitch);
        }



        /// <summary>
        ///     Adds a sink that writes log events to a Azure CosmosDB table in the provided endpoint.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="options">The cosmos configuration options to use for operations</param>
        /// <param name="restrictedToMinimumLevel">The minimum log event level required in order to write an event to the sink.</param>
        /// <param name="levelSwitch">
        /// A switch allowing the pass-through minimum level to be changed at runtime.
        /// </param>
        /// <exception cref="ArgumentNullException">A required parameter is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">A required parameter value is out of acceptable range.</exception>
        public static LoggerConfiguration AzCosmosDB(
            this LoggerSinkConfiguration loggerConfiguration,
            AzCosmosDbSinkOptions options,
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            LoggingLevelSwitch levelSwitch = null)
        {
            if (loggerConfiguration == null)
            {
                throw new ArgumentNullException(nameof(loggerConfiguration));
            }

            if (string.IsNullOrWhiteSpace(options.EndpointUri?.ToString()))
            {
                throw new ArgumentNullException(nameof(options.EndpointUri));
            }

            if (string.IsNullOrWhiteSpace(options.AuthorizationKey))
            {
                throw new ArgumentNullException(nameof(options.AuthorizationKey));
            }

            if ((options.TimeToLive != null) && (options.TimeToLive.Value.TotalSeconds > TimeSpan.FromDays(24_855).TotalSeconds))
            {
                throw new ArgumentOutOfRangeException(nameof(options.TimeToLive));
            }

            var batchingOptions = new PeriodicBatchingSinkOptions
            {
                BatchSizeLimit = options.BatchPostingLimit,
                Period = options.Period,
                EagerlyEmitFirstEvent = true,
                QueueLimit = options.QueueSizeLimit
            };

            var cosmosSink = new AzCosmosDbSink(options);
            var batchingSink = new PeriodicBatchingSink(cosmosSink, batchingOptions);
            return loggerConfiguration.Sink(batchingSink,
                restrictedToMinimumLevel,
                levelSwitch);
        }
    }
}
