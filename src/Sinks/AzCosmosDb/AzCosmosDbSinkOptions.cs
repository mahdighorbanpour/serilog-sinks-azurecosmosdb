using System;
using System.Collections.Generic;
using System.Text;

namespace Serilog.Sinks.AzCosmosDB
{
    public class AzCosmosDbSinkOptions
    {
        private int _queueSizeLimit;
        ///<summary>
        /// The maximum number of events to post in a single batch. Defaults to: 50.
        /// </summary>
        public int BatchPostingLimit { get; set; }
        ///<summary>
        /// The time to wait between checking for event batches. Defaults to 2 seconds.
        /// </summary>
        public TimeSpan Period { get; set; }

        /// <summary>
        /// Disables SSL for use with locally hosted CosmosDB instances. This should not be used in production!
        /// </summary>
        public bool DisableSSL { get; set; }


        /// <summary>
        /// The maximum number of events that will be held in-memory while waiting to ship them to
        /// Cosmos. Beyond this limit, events will be dropped. The default is 100,000. 
        /// </summary>
        public int QueueSizeLimit
        {
            get { return _queueSizeLimit; }
            set
            {
                if (value < 0)
                    throw new ArgumentOutOfRangeException(nameof(QueueSizeLimit), "Queue size limit must be non-zero.");
                _queueSizeLimit = value;
            }
        }

        public Uri EndpointUri { get; set; }
        public string AuthorizationKey { get; set; }
        public string DatabaseName { get; set; }
        public string CollectionName { get; set; }
        public string PartitionKey { get; set; }
        public IPartitionKeyProvider PartitionKeyProvider { get; set; }
        public IFormatProvider FormatProvider { get; set; }
        public bool StoreTimestampInUTC { get; set; }
        public TimeSpan? TimeToLive { get; set; }

        public AzCosmosDbSinkOptions()
        {
            this.Period = TimeSpan.FromSeconds(2);
            this.BatchPostingLimit = 100;
            this.QueueSizeLimit = 25000;
            this.DatabaseName = "Diagnostics";
            this.CollectionName = "Logs";
            this.PartitionKey = "UtcDate";
            this.StoreTimestampInUTC = true;
        }
    }
}
