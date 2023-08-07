using System.Collections.Concurrent;
using Couchbase;
using Couchbase.KeyValue;
using Microsoft.Extensions.Logging;
using NET_App.Models;

namespace NET_App.Workloads;

public class KeyValueWorkload
{
    private ICouchbaseCollection[] _collections;
    private int _collectionsLength;
    private KvOperation _operation;

    private const string DocPrefix = "DOTNET-KV-";

    private ConcurrentDictionary<string, int> results = new();

    public ConcurrentDictionary<string, int> Results => results;

    public enum KvOperation
    {
        Insert,
        Update,
        Get,
        Delete
    }

    public KeyValueWorkload(ICouchbaseCollection[] collections, KvOperation operation)
    {
        _collections = collections;
        _operation = operation;
        _collectionsLength = collections.Length;
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
                var collectionForOp = _collections[i % _collectionsLength];
                var task = _operation switch
                {
                    KvOperation.Insert => collectionForOp.InsertAsync(docId, document),
                    KvOperation.Update => collectionForOp.UpsertAsync(docId, document),
                    KvOperation.Get => collectionForOp.GetAsync(docId),
                    KvOperation.Delete => collectionForOp.RemoveAsync(docId),
                    _ => collectionForOp.UpsertAsync(docId, document)
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

    public async Task PrintResultsPeriodically(CancellationToken cancellationToken)
    {
        var timer = new PeriodicTimer(TimeSpan.FromMinutes(1));

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                PrintResults();
            }
        }
        catch(OperationCanceledException)
        {
            
        }

    }

    public void PrintResults()
    {
        Console.WriteLine(DateTime.Now.ToString("t") + " Operation Results:");
        results.Select(i => $"{i.Key}: {i.Value}").ToList().ForEach(Console.WriteLine);
    }
    
}