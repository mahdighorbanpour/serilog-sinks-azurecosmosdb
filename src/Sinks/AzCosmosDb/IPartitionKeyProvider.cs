using System;
using Serilog.Events;

namespace Serilog.Sinks.AzCosmosDB
{
    /// <summary>
    /// Generates a string value to be used for the partition key
    /// </summary>
    public interface IPartitionKeyProvider
    {
        string GeneratePartitionKey(LogEvent logEvent);
    }

    /// <summary>
    /// Generates a string value to be used for the partition key
    /// </summary>
    public class DefaultPartitionKeyProvider : IPartitionKeyProvider
    {
        private readonly IFormatProvider _formatProvider;

        public DefaultPartitionKeyProvider(IFormatProvider formatProvider)
        {
            _formatProvider = formatProvider;
        }
        public string GeneratePartitionKey(LogEvent logEvent)
        {
            return logEvent?.Timestamp.ToString("dd.MM.yyyy", _formatProvider);
        }
    }
}
