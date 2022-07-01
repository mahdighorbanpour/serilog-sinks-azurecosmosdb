using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Serilog.Events;

namespace Serilog.Sinks.AzCosmosDB.TestRunner
{
    public class PartitionKeyProvider: IPartitionKeyProvider
    {
        public string GeneratePartitionKey(LogEvent logEvent)
        {
            return logEvent.Properties["guid"].ToString();
        }
    }
}
