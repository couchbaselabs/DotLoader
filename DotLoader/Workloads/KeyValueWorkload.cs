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
        var id = start;
        while (!cancellationToken.IsCancellationRequested && (id < numDocs + start || forTime))
        {
            try
            {
                var docId = DocPrefix + id;
                var document = PersonGenerator.GenerateDocument(docSize);
                switch (_operation)
                {
                    case KvOperation.Insert : await _collection.InsertAsync(docId, document);
                        break;
                    case KvOperation.Update : await _collection.UpsertAsync(docId, document);
                        break;
                    case KvOperation.Get : await _collection.GetAsync(docId);
                        break;
                    case KvOperation.Delete : await _collection.RemoveAsync(docId);
                        break;
                }
                results.AddOrUpdate("success", 1, (k, existingValue) => existingValue + 1);

            }
            catch (Exception e)
            {
                results.AddOrUpdate(e.GetType().ToString(), 1, (k, existingValue) => existingValue + 1);
            }
            id++;
            if (forTime && id == numDocs + start)
            {
                id = start;
            }
        }
    }
    
}