using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Ingest;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace testing
{
    class Program
    {
        public static void Main(string[] args)
        {
            KustoConnectionStringBuilder readStringBuilder;
            KustoConnectionStringBuilder ingestStringBuilder;
            readStringBuilder = new KustoConnectionStringBuilder(@"https://kuskusops.kustomfa.windows.net")
            {
                
            FederatedSecurity = true,
                InitialCatalog = "testdb", 
                Authority = "72f988bf-86f1-41af-91ab-2d7cd011db47"
            };

            ingestStringBuilder = new KustoConnectionStringBuilder($"https://ingest-kuskusops.kustomfa.windows.net")
            {
                FederatedSecurity = true,
                InitialCatalog = "testdb",
                Authority = "72f988bf-86f1-41af-91ab-2d7cd011db47"
            };
            using (IKustoIngestClient kustoIngestClient = KustoIngestFactory.CreateQueuedIngestClient(ingestStringBuilder))
            {
                var kustoIngestionProperties = new KustoQueuedIngestionProperties("testdb", "test_table");
                var dataReader = GetDataAsIDataReader();
                IKustoIngestionResult ingestionResult =
                    kustoIngestClient.IngestFromDataReader(dataReader, kustoIngestionProperties);
                var ingestionStatus = ingestionResult.GetIngestionStatusCollection().First();
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var shouldContinue = true;
                while ((ingestionStatus.Status == Status.Pending) && (shouldContinue))
                {
                    // Wait a minute...
                    Thread.Sleep(TimeSpan.FromMinutes(1));
                    // Try again
                    ingestionStatus = ingestionResult.GetIngestionStatusBySourceId(ingestionStatus.IngestionSourceId);
                    shouldContinue = watch.ElapsedMilliseconds < 360000;
                }

                watch.Stop();
                if (ingestionStatus.Status == Status.Pending)
                {
                    // The status of the ingestion did not change.
                    Console.WriteLine(
                        "Ingestion with ID:'{0}' did not complete. Timed out after :'{1}' Milliseconds"
                            .FormatWithInvariantCulture(
                                ingestionStatus.IngestionSourceId, watch.ElapsedMilliseconds));
                }
                else
                {
                    // The status of the ingestion has changed
                    Console.WriteLine(
                        "Ingestion with ID:'{0}' is complete. Ingestion Status:'{1}'".FormatWithInvariantCulture(
                            ingestionStatus.IngestionSourceId, ingestionStatus.Status));
                }
            }
            // 4. Show the contents of the table
            using (var client = Kusto.Data.Net.Client.KustoClientFactory.CreateCslQueryProvider(readStringBuilder))
            {
                while (true)
                {
                    var query = "test_table | count";
                    var reader = client.ExecuteQuery(query);
                    Kusto.Cloud.Platform.Data.ExtendedDataReader.WriteAsText(reader, "Data ingested into the table:", tabify: true, firstOnly: true);
                    Console.WriteLine("Press 'r' to retry retrieving data from the table, any other key to quit");
                    var key = Console.ReadKey();
                    if (key.KeyChar != 'r' || key.KeyChar != 'R')
                    {
                        break;
                    }
                }
            }
        }
        private static IDataReader GetDataAsIDataReader()
        {
            List<Row> data = new List<Row>();
            data.Add(new Row("ABCD", "Linecard", "Requested"));
            data.Add(new Row("ABCD", "Linecard", "Requested"));
            var properties = data.First().GetType().GetProperties();
            string[] fields = new string[properties.Length];
            for (int i = 0; i < properties.Length; i++)
            {
                fields[i] = properties[i].Name;
            }
            var ret = new Kusto.Cloud.Platform.Data.EnumerableDataReader<Row>(data, "Device", "Operation", "Status");
            return ret;
        }
        public class Row
        {
            public string Device { get; set; }
            public string Operation { get; set; }
            public string Status { get; set; }
            public string Icm { get; set; }
            public string Message { get; set; }
            public DateTime UpdatedTimestamp { get; set; }
            public Row(string d, string o, string s)
            {
                this.Device = d;
                this.Operation = o;
                this.Status = s;
            }
            
        }
    }
}
