using System.Collections.Concurrent;
using Couchbase;
using Couchbase.KeyValue;
using Microsoft.Extensions.Logging;
using NET_App.Models;

namespace NET_App.Workloads;

public class KeyValueWorkload
{
    private ICluster _cluster;
    private IBucket _bucket;
    private IScope _scope;
    private ICouchbaseCollection _collection;
    private ILogger _logger;
    private KvOperation _operation;

    private const string DocPrefix = "DOTNET-KV-";

    private ConcurrentDictionary<string, int> results = new ConcurrentDictionary<string, int>();

    public ConcurrentDictionary<string, int> Results => results;

    public enum KvOperation
    {
        Insert,
        Update,
        Get,
        Delete
    }

    public KeyValueWorkload(ICluster cluster, IBucket bucket, IScope scope, ICouchbaseCollection collection, ILogger logger, KvOperation operation)
    {
        _cluster = cluster;
        _bucket = bucket;
        _scope = scope;
        _collection = collection;
        _operation = operation;
        _logger = logger;
    }
    

    /**
     * @forTime - keep running the operations until the cancellation token indicates to stop.
     *              Note only really applicable for Get/Update
     */
    public async Task StartAsync(int numDocs, int docSize, int start, bool forTime, CancellationToken cancellationToken)
    {
        var batchSize = 1000;
        var startBatch = start;
        while (!cancellationToken.IsCancellationRequested && (startBatch < numDocs + start || forTime))
        {
            var tasks = new List<Task>();
            var document = PersonGenerator.GenerateDocument(docSize);

            for (var i = startBatch; i < startBatch + batchSize; i++)
            {
                var docId = DocPrefix + i;
                var task = _operation switch
                {
                    KvOperation.Insert => _collection.InsertAsync(docId, document),
                    KvOperation.Update => _collection.UpsertAsync(docId, document),
                    KvOperation.Get => _collection.GetAsync(docId),
                    KvOperation.Delete => _collection.RemoveAsync(docId),
                    _ => _collection.UpsertAsync(docId, document)
                };
                tasks.Add(task);
            }
            try
            {
                await Task.WhenAll(tasks);
            }
            catch (Exception e)
            {
                results.AddOrUpdate(e.GetType().ToString(), 1, (k, existingValue) => existingValue + 1);
            }

            foreach (var task in tasks)
            {
                if (task.IsCompletedSuccessfully)
                {
                    results.AddOrUpdate("success", 1, (k, existingValue) => existingValue + 1);
                }
                else
                {
                    results.AddOrUpdate(task.Exception.InnerExceptions.GetType().ToString(), 1, (k, existingValue) => existingValue + 1);
                }
            }
            startBatch += batchSize;
            if (forTime && startBatch >= numDocs + start)
            {
                startBatch = start;
            }
        }
    }
    
}